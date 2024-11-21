import tkinter as tk
from tkinter import ttk
from tkinter_gui.layout import create_scrollable_frame, create_selected_count_label
from tkinter_gui.event_handlers import update_selected_count, on_mouse_wheel

def create_gui(columns, read_data_func, generate_parquet_file_callback):
    root = tk.Tk()
    root.title("Select Fields for Parquet")

    canvas, scrollable_frame = create_scrollable_frame(root)
    selected_count_label = create_selected_count_label(root)

    column_checkboxes = {}

    preselected_columns = ["FILEID", "STUSAB", "LOGRECNO", "YEAR", "SUMLEV"]
    
    selected_count_label.config(text=f"Selected Fields: {len(preselected_columns)}")
    for column in columns:
        column_checkboxes[column] = tk.BooleanVar()

        column_checkboxes[column].set(column in preselected_columns)

        checkbox = ttk.Checkbutton(
            scrollable_frame, text=column, variable=column_checkboxes[column]
        )
        checkbox.pack(anchor="w")

    checkbox_frame = ttk.Frame(root)
    checkbox_frame.pack(padx=10, pady=10, fill=tk.BOTH, expand=True)

    for column_var in column_checkboxes.values():
        column_var.trace_add("write", lambda *args: update_selected_count(selected_count_label, lambda: [x for x in column_checkboxes.keys() if column_checkboxes[x].get() ]))

    canvas.bind("<MouseWheel>", lambda event: on_mouse_wheel(event, canvas))


    generate_button = ttk.Button(root, text="Generate Parquet File", command=lambda: generate_parquet_file_callback(column_checkboxes, read_data_func))
    generate_button.pack(pady=20)

    root.mainloop()
