import base64
import json
import threading
import tkinter as tk
from dataclasses import dataclass
from pathlib import Path
from tkinter import ttk, filedialog, messagebox
from typing import Any, Dict, List, Optional, Tuple

import fastavro


def _json_safe(value: Any) -> Any:
    """Convert values to JSON-serializable equivalents."""
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, bytes):
        # Encode bytes as base64 string with a marker.
        return {"__bytes_b64__": base64.b64encode(value).decode("ascii")}
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    # Fallback: string representation
    return str(value)


def record_to_row(record: Dict[str, Any], field_names: List[str]) -> Tuple[Any, ...]:
    def cell(v: Any) -> str:
        if v is None:
            return ""
        if isinstance(v, (str, int, float, bool)):
            return str(v)
        if isinstance(v, bytes):
            # show short preview
            b64 = base64.b64encode(v).decode("ascii")
            return f"<bytes b64:{b64[:24]}...>"
        # nested structures as compact json
        try:
            return json.dumps(_json_safe(v), ensure_ascii=False)
        except Exception:
            return str(v)

    return tuple(cell(record.get(f)) for f in field_names)


@dataclass
class AvroState:
    file_path: Optional[Path] = None
    schema: Optional[Dict[str, Any]] = None
    field_names: List[str] = None
    page_size: int = 50
    page_index: int = 0  # 0-based
    current_records: List[Dict[str, Any]] = None


class AvroViewerApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Avro Viewer (local)")

        self.state = AvroState(field_names=[], current_records=[])

        self._build_ui()

    # ---------------- UI ----------------

    def _build_ui(self):
        self.root.geometry("1200x750")

        top = ttk.Frame(self.root, padding=8)
        top.pack(side=tk.TOP, fill=tk.X)

        ttk.Button(top, text="Open .avro…", command=self.open_file).pack(side=tk.LEFT)

        ttk.Label(top, text="Page size:").pack(side=tk.LEFT, padx=(12, 4))
        self.page_size_var = tk.StringVar(value=str(self.state.page_size))
        page_size_entry = ttk.Entry(top, width=6, textvariable=self.page_size_var)
        page_size_entry.pack(side=tk.LEFT)
        ttk.Button(top, text="Apply", command=self.apply_page_size).pack(side=tk.LEFT, padx=(6, 0))

        # Search controls
        ttk.Separator(top, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=10)

        ttk.Label(top, text="Search:").pack(side=tk.LEFT, padx=(0, 4))
        self.search_var = tk.StringVar()
        ttk.Entry(top, width=30, textvariable=self.search_var).pack(side=tk.LEFT)

        ttk.Label(top, text="Field:").pack(side=tk.LEFT, padx=(10, 4))
        self.field_var = tk.StringVar(value="(all)")
        self.field_combo = ttk.Combobox(top, width=18, textvariable=self.field_var, state="readonly", values=["(all)"])
        self.field_combo.pack(side=tk.LEFT)

        self.max_results_var = tk.StringVar(value="500")
        ttk.Label(top, text="Max results:").pack(side=tk.LEFT, padx=(10, 4))
        ttk.Entry(top, width=6, textvariable=self.max_results_var).pack(side=tk.LEFT)

        ttk.Button(top, text="Run search", command=self.run_search).pack(side=tk.LEFT, padx=(10, 0))
        ttk.Button(top, text="Clear search", command=self.clear_search).pack(side=tk.LEFT, padx=(6, 0))

        # Export controls
        ttk.Separator(top, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=10)
        ttk.Button(top, text="Export current → JSON", command=self.export_json).pack(side=tk.LEFT)
        ttk.Button(top, text="Export current → CSV", command=self.export_csv).pack(side=tk.LEFT, padx=(6, 0))

        # Middle: schema + table
        mid = ttk.Panedwindow(self.root, orient=tk.VERTICAL)
        mid.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=8, pady=8)

        # Schema viewer
        schema_frame = ttk.Labelframe(mid, text="Schema", padding=6)
        self.schema_text = tk.Text(schema_frame, height=10, wrap="none")
        schema_y = ttk.Scrollbar(schema_frame, orient="vertical", command=self.schema_text.yview)
        schema_x = ttk.Scrollbar(schema_frame, orient="horizontal", command=self.schema_text.xview)
        self.schema_text.configure(yscrollcommand=schema_y.set, xscrollcommand=schema_x.set)

        self.schema_text.grid(row=0, column=0, sticky="nsew")
        schema_y.grid(row=0, column=1, sticky="ns")
        schema_x.grid(row=1, column=0, sticky="ew")
        schema_frame.rowconfigure(0, weight=1)
        schema_frame.columnconfigure(0, weight=1)

        # Table viewer
        table_frame = ttk.Labelframe(mid, text="Records (table)", padding=6)

        self.tree = ttk.Treeview(table_frame, columns=(), show="headings")
        self.tree.grid(row=0, column=0, sticky="nsew")

        tree_y = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        tree_x = ttk.Scrollbar(table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=tree_y.set, xscrollcommand=tree_x.set)
        tree_y.grid(row=0, column=1, sticky="ns")
        tree_x.grid(row=1, column=0, sticky="ew")

        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)

        mid.add(schema_frame, weight=1)
        mid.add(table_frame, weight=4)

        # Bottom: paging + status
        bottom = ttk.Frame(self.root, padding=8)
        bottom.pack(side=tk.BOTTOM, fill=tk.X)

        self.prev_btn = ttk.Button(bottom, text="◀ Prev", command=self.prev_page, state=tk.DISABLED)
        self.next_btn = ttk.Button(bottom, text="Next ▶", command=self.next_page, state=tk.DISABLED)
        self.prev_btn.pack(side=tk.LEFT)
        self.next_btn.pack(side=tk.LEFT, padx=(6, 0))

        self.page_label = ttk.Label(bottom, text="Page: -")
        self.page_label.pack(side=tk.LEFT, padx=(12, 0))

        self.status_var = tk.StringVar(value="Open an Avro file to begin.")
        ttk.Label(bottom, textvariable=self.status_var).pack(side=tk.RIGHT)

        # Double click row → detail JSON
        self.tree.bind("<Double-1>", self.show_selected_record_json)

    # --------------- Core ops ---------------

    def set_status(self, msg: str):
        self.status_var.set(msg)

    def open_file(self):
        path = filedialog.askopenfilename(
            title="Open Avro file",
            filetypes=[("Avro files", "*.avro"), ("All files", "*.*")]
        )
        if not path:
            return
        self.load_avro(Path(path))

    def apply_page_size(self):
        try:
            ps = int(self.page_size_var.get().strip())
            if ps < 1 or ps > 5000:
                raise ValueError
        except Exception:
            messagebox.showerror("Invalid page size", "Please enter a number between 1 and 5000.")
            return
        self.state.page_size = ps
        self.state.page_index = 0
        if self.state.file_path:
            self.refresh_page()

    def load_avro(self, path: Path):
        try:
            with path.open("rb") as fo:
                reader = fastavro.reader(fo)
                schema = reader.writer_schema
        except Exception as e:
            messagebox.showerror("Failed to open Avro", str(e))
            return

        field_names = []
        if isinstance(schema, dict) and "fields" in schema and isinstance(schema["fields"], list):
            field_names = [f.get("name", "") for f in schema["fields"] if isinstance(f, dict) and f.get("name")]
        else:
            # Some schemas can be nested; fallback:
            field_names = []

        self.state.file_path = path
        self.state.schema = schema
        self.state.field_names = field_names
        self.state.page_index = 0
        self.state.current_records = []

        self._render_schema()
        self._setup_table_columns()

        self.field_combo["values"] = ["(all)"] + field_names
        self.field_var.set("(all)")

        self.refresh_page()

    def _render_schema(self):
        self.schema_text.delete("1.0", tk.END)
        if not self.state.schema:
            return
        try:
            txt = json.dumps(self.state.schema, ensure_ascii=False, indent=2)
        except Exception:
            txt = str(self.state.schema)
        self.schema_text.insert(tk.END, txt)

    def _setup_table_columns(self):
        # Clear existing
        for col in self.tree["columns"]:
            self.tree.heading(col, text="")
        self.tree["columns"] = tuple(self.state.field_names)
        self.tree.delete(*self.tree.get_children())

        for col in self.state.field_names:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=150, anchor=tk.W, stretch=True)

    def _read_page(self, page_index: int) -> List[Dict[str, Any]]:
        """Read a page by reopening and skipping (simple & reliable)."""
        assert self.state.file_path is not None
        page_size = self.state.page_size
        start = page_index * page_size
        end = start + page_size
        records: List[Dict[str, Any]] = []
        with self.state.file_path.open("rb") as fo:
            reader = fastavro.reader(fo)
            # skip start
            i = 0
            try:
                for rec in reader:
                    if i >= end:
                        break
                    if i >= start:
                        records.append(rec)
                    i += 1
            except Exception:
                pass
        return records

    def refresh_page(self):
        if not self.state.file_path:
            return
        self.set_status("Loading page…")
        self.root.update_idletasks()

        page = self._read_page(self.state.page_index)
        self.state.current_records = page
        self._render_records(page)

        # Buttons
        self.prev_btn.configure(state=(tk.NORMAL if self.state.page_index > 0 else tk.DISABLED))
        self.next_btn.configure(state=(tk.NORMAL if len(page) == self.state.page_size else tk.DISABLED))

        self.page_label.configure(text=f"Page: {self.state.page_index + 1}  (size {self.state.page_size})")
        self.set_status(f"Loaded {len(page)} records from {self.state.file_path.name}")

    def _render_records(self, records: List[Dict[str, Any]]):
        self.tree.delete(*self.tree.get_children())
        if not self.state.field_names:
            # fallback: show keys from first record
            if records:
                self.state.field_names = sorted({k for r in records for k in r.keys()})
                self._setup_table_columns()
                self.field_combo["values"] = ["(all)"] + self.state.field_names
        for r in records:
            self.tree.insert("", "end", values=record_to_row(r, self.state.field_names))

    def next_page(self):
        self.state.page_index += 1
        self.refresh_page()

    def prev_page(self):
        if self.state.page_index == 0:
            return
        self.state.page_index -= 1
        self.refresh_page()

    # --------------- Search ---------------

    def clear_search(self):
        self.search_var.set("")
        self.state.page_index = 0
        if self.state.file_path:
            self.refresh_page()

    def run_search(self):
        if not self.state.file_path:
            messagebox.showinfo("No file", "Open an Avro file first.")
            return

        query = self.search_var.get().strip()
        if not query:
            messagebox.showinfo("Empty query", "Type something to search.")
            return

        field = self.field_var.get()
        field_name = None if field == "(all)" else field

        try:
            max_results = int(self.max_results_var.get().strip())
            if max_results < 1 or max_results > 200000:
                raise ValueError
        except Exception:
            messagebox.showerror("Invalid max results", "Max results must be between 1 and 200000.")
            return

        # UI prep (main thread)
        self.set_status("Searching…")
        self.prev_btn.configure(state=tk.DISABLED)
        self.next_btn.configure(state=tk.DISABLED)
        self.tree.delete(*self.tree.get_children())
        self.state.current_records = []

        # Snapshot state for this search (avoid races if user opens another file mid-search)
        search_path = self.state.file_path
        search_fields = list(self.state.field_names)  # keep stable
        search_query = query
        search_field_name = field_name

        # Counters stored in outer scope (shared by UI callbacks, but mutated only in main thread)
        counters = {"checked": 0, "found": 0}

        BATCH_SIZE = 50  # tune: 20..200 typically OK

        def add_batch(batch_records, checked_snapshot, done=False):
            """
            Runs in main thread only.
            Inserts rows to Treeview and appends records to state.current_records.
            """
            # If user already opened another file, ignore stale results
            if self.state.file_path != search_path:
                return

            # Append records to current view storage (for export + double click)
            self.state.current_records.extend(batch_records)

            # Insert rows
            for rec in batch_records:
                self.tree.insert("", "end", values=record_to_row(rec, search_fields))

            # Update counters (main thread)
            counters["checked"] = checked_snapshot
            counters["found"] = len(self.state.current_records)

            self.page_label.configure(
                text=f"Found: {counters['found']} (checked {counters['checked']:,})"
            )

            if done:
                self.set_status(f"Search done: {counters['found']} matches")
                self.page_label.configure(
                    text=f"Search results: {counters['found']} (checked {counters['checked']:,})"
                )
                # paging stays disabled in search view (as you had)
                self.prev_btn.configure(state=tk.DISABLED)
                self.next_btn.configure(state=tk.DISABLED)

        def worker():
            batch = []
            checked = 0
            found = 0

            try:
                with search_path.open("rb") as fo:
                    reader = fastavro.reader(fo)
                    for rec in reader:
                        checked += 1

                        if self._match(rec, search_field_name, search_query):
                            batch.append(rec)
                            found += 1

                            # flush batch
                            if len(batch) >= BATCH_SIZE:
                                to_send = batch
                                batch = []
                                self.root.after(0, lambda b=to_send, c=checked: add_batch(b, c, done=False))

                            if found >= max_results:
                                break

            except Exception as e:
                self.root.after(0, lambda: messagebox.showerror("Search failed", str(e)))
                self.root.after(0, lambda: self.set_status("Search failed"))
                return

            # flush remaining
            if batch:
                self.root.after(0, lambda b=batch, c=checked: add_batch(b, c, done=False))

            # finalize
            self.root.after(0, lambda c=checked: add_batch([], c, done=True))

        threading.Thread(target=worker, daemon=True).start()

    def _match(self, rec: Dict[str, Any], field_name: Optional[str], query: str) -> bool:
        q = query.lower()
        if field_name is None:
            for v in rec.values():
                if v is None:
                    continue
                try:
                    if q in str(v).lower():
                        return True
                except Exception:
                    continue
            return False
        v = rec.get(field_name)
        if v is None:
            return False
        try:
            return q in str(v).lower()
        except Exception:
            return False

    # --------------- Export ---------------

    def export_json(self):
        if not self.state.current_records:
            messagebox.showinfo("Nothing to export", "No records currently shown.")
            return
        path = filedialog.asksaveasfilename(
            title="Save JSON",
            defaultextension=".json",
            filetypes=[("JSON", "*.json"), ("All files", "*.*")]
        )
        if not path:
            return
        try:
            safe = [_json_safe(r) for r in self.state.current_records]
            with open(path, "w", encoding="utf-8") as f:
                json.dump(safe, f, ensure_ascii=False, indent=2)
            self.set_status(f"Exported JSON: {Path(path).name}")
        except Exception as e:
            messagebox.showerror("Export failed", str(e))

    def export_csv(self):
        if not self.state.current_records:
            messagebox.showinfo("Nothing to export", "No records currently shown.")
            return
        if not self.state.field_names:
            messagebox.showinfo("No columns", "Cannot export CSV without columns.")
            return
        path = filedialog.asksaveasfilename(
            title="Save CSV",
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv"), ("All files", "*.*")]
        )
        if not path:
            return
        try:
            with open(path, "w", encoding="utf-8", newline="") as f:
                import csv
                w = csv.writer(f)
                w.writerow(self.state.field_names)
                for r in self.state.current_records:
                    row = [record_to_row(r, self.state.field_names)[i] for i in range(len(self.state.field_names))]
                    w.writerow(row)
            self.set_status(f"Exported CSV: {Path(path).name}")
        except Exception as e:
            messagebox.showerror("Export failed", str(e))

    # --------------- Row details ---------------

    def show_selected_record_json(self, _event=None):
        sel = self.tree.selection()
        if not sel:
            return
        idx = self.tree.index(sel[0])
        if idx < 0 or idx >= len(self.state.current_records):
            return
        rec = self.state.current_records[idx]
        safe = _json_safe(rec)
        txt = json.dumps(safe, ensure_ascii=False, indent=2)

        win = tk.Toplevel(self.root)
        win.title("Record (JSON)")
        win.geometry("800x600")
        t = tk.Text(win, wrap="none")
        y = ttk.Scrollbar(win, orient="vertical", command=t.yview)
        x = ttk.Scrollbar(win, orient="horizontal", command=t.xview)
        t.configure(yscrollcommand=y.set, xscrollcommand=x.set)
        t.grid(row=0, column=0, sticky="nsew")
        y.grid(row=0, column=1, sticky="ns")
        x.grid(row=1, column=0, sticky="ew")
        win.rowconfigure(0, weight=1)
        win.columnconfigure(0, weight=1)
        t.insert(tk.END, txt)
        t.configure(state=tk.NORMAL)


def main():
    root = tk.Tk()
    app = AvroViewerApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
