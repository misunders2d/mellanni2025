import sys
from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QPushButton,
    QLabel,
    QLineEdit,
    QCheckBox,
    QRadioButton,
    QComboBox,
    QSpinBox,
    QDoubleSpinBox,
    QSlider,
    QProgressBar,
    QListWidget,
    QTextEdit,
    QGroupBox,
    QTabWidget,
    QToolButton,
    QCalendarWidget,
    QDateEdit,
    QTimeEdit,
    QDial,
    QScrollArea,
)
from PySide6.QtCore import Qt


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("PySide6 Widget Showcase")
        self.setGeometry(100, 100, 800, 600)

        # Main widget and layout
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QHBoxLayout(
            main_widget
        )  # Horizontal layout to split into columns

        # Left column layout
        left_layout = QVBoxLayout()
        main_layout.addLayout(left_layout)

        # Right column layout (inside a scroll area for overflow)
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        scroll = QScrollArea()
        scroll.setWidget(right_widget)
        scroll.setWidgetResizable(True)
        main_layout.addWidget(scroll)

        # --- Left Column Widgets ---
        # QLabel: Simple text display
        label = QLabel("Welcome to PySide6 Widgets!")
        label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        left_layout.addWidget(label)

        # QPushButton: Clickable button
        button = QPushButton("Click Me!")
        button.clicked.connect(lambda: label.setText("Button Clicked!"))
        left_layout.addWidget(button)

        # QLineEdit: Single-line text input
        line_edit = QLineEdit()
        line_edit.setPlaceholderText("Enter text here")
        left_layout.addWidget(line_edit)

        # QCheckBox: Toggleable checkbox
        checkbox = QCheckBox("Enable Feature")
        checkbox.stateChanged.connect(
            lambda state: label.setText("Checked" if state else "Unchecked")
        )
        left_layout.addWidget(checkbox)

        # QRadioButton: Exclusive radio button group
        radio_group = QGroupBox("Radio Options")
        radio_layout = QVBoxLayout()
        radio1 = QRadioButton("Option 1")
        radio2 = QRadioButton("Option 2")
        radio_layout.addWidget(radio1)
        radio_layout.addWidget(radio2)
        radio_group.setLayout(radio_layout)
        left_layout.addWidget(radio_group)

        # QComboBox: Dropdown menu
        combo = QComboBox()
        combo.addItems(["Choice A", "Choice B", "Choice C"])
        combo.currentTextChanged.connect(
            lambda text: label.setText(f"Selected: {text}")
        )
        left_layout.addWidget(combo)

        # QSpinBox: Integer input with up/down arrows
        spinbox = QSpinBox()
        spinbox.setRange(0, 100)
        spinbox.setValue(50)
        left_layout.addWidget(spinbox)

        # QDoubleSpinBox: Float input with up/down arrows
        double_spinbox = QDoubleSpinBox()
        double_spinbox.setRange(0.0, 10.0)
        double_spinbox.setSingleStep(0.1)
        double_spinbox.setValue(5.0)
        left_layout.addWidget(double_spinbox)

        # --- Right Column Widgets ---
        # QSlider: Horizontal slider
        slider = QSlider(Qt.Orientation.Horizontal)
        slider.setRange(0, 100)
        slider.setValue(50)
        slider.valueChanged.connect(spinbox.setValue)  # Link to spinbox
        right_layout.addWidget(slider)

        # QProgressBar: Progress indicator
        progress = QProgressBar()
        progress.setRange(0, 100)
        slider.valueChanged.connect(progress.setValue)  # Link to slider
        right_layout.addWidget(progress)

        # QListWidget: Multi-selectable list
        list_widget = QListWidget()
        list_widget.addItems(["Item 1", "Item 2", "Item 3"])
        list_widget.setSelectionMode(QListWidget.SelectionMode.MultiSelection)
        list_widget.itemSelectionChanged.connect(
            lambda: label.setText(
                f"Selected: {[item.text() for item in list_widget.selectedItems()]}"
            )
        )
        right_layout.addWidget(list_widget)

        # QTextEdit: Multi-line text editor
        text_edit = QTextEdit()
        text_edit.setPlaceholderText("Type some text here...")
        right_layout.addWidget(text_edit)

        # QTabWidget: Tabbed interface
        tab_widget = QTabWidget()
        tab1 = QWidget()
        tab1_layout = QVBoxLayout()
        tab1_layout.addWidget(QLabel("This is Tab 1"))
        tab1.setLayout(tab1_layout)
        tab2 = QWidget()
        tab2_layout = QVBoxLayout()
        tab2_layout.addWidget(QLabel("This is Tab 2"))
        tab2.setLayout(tab2_layout)
        tab_widget.addTab(tab1, "Tab 1")
        tab_widget.addTab(tab2, "Tab 2")
        right_layout.addWidget(tab_widget)

        # QToolButton: Button with icon-like behavior
        tool_button = QToolButton()
        tool_button.setText("Tool")
        tool_button.clicked.connect(lambda: label.setText("Tool Button Clicked!"))
        right_layout.addWidget(tool_button)

        # QCalendarWidget: Date picker
        calendar = QCalendarWidget()
        right_layout.addWidget(calendar)

        # QDateEdit: Editable date input
        date_edit = QDateEdit()
        date_edit.setCalendarPopup(True)
        right_layout.addWidget(date_edit)

        # QTimeEdit: Editable time input
        time_edit = QTimeEdit()
        right_layout.addWidget(time_edit)

        # QDial: Circular dial control
        dial = QDial()
        dial.setRange(0, 100)
        dial.valueChanged.connect(progress.setValue)  # Link to progress bar
        right_layout.addWidget(dial)

        # Stretch to push content up in layouts
        left_layout.addStretch()
        right_layout.addStretch()


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MainWindow()
    window.show()
    sys.exit(app.exec())
