def update_selected_count(label, get_selected_columns):
    """
    Update the label showing the count of selected fields.
    """
    count = len(get_selected_columns())
    label.config(text=f"Selected Fields: {count}")

def on_mouse_wheel(event, canvas):
    """
    Scroll the canvas vertically using the mouse wheel.
    """
    canvas.yview_scroll(-1 * (event.delta // 120), "units")