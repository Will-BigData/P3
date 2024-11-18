import tkinter as tk
from tkinter import ttk

def create_scrollable_frame(root):
    """
    Create a scrollable frame for displaying checkboxes.
    """
    main_frame = ttk.Frame(root)
    main_frame.pack(fill=tk.BOTH, expand=True)

    canvas = tk.Canvas(main_frame)
    canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    scrollbar = ttk.Scrollbar(main_frame, orient=tk.VERTICAL, command=canvas.yview)
    scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

    canvas.configure(yscrollcommand=scrollbar.set)
    canvas.bind(
        "<Configure>",
        lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
    )

    scrollable_frame = ttk.Frame(canvas)
    canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")

    return canvas, scrollable_frame

def create_save_button(root, save_command):
    """
    Create a save button for saving selected fields.
    """
    button = ttk.Button(root, text="Save Selected Fields", command=save_command)
    button.pack(pady=10)
    return button

def create_selected_count_label(root):
    """
    Create a label to display the count of selected fields.
    """
    label = ttk.Label(root, text="Selected Fields: 0", font=("Arial", 12))
    label.pack(pady=(10, 0))
    return label
