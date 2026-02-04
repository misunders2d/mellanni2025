import sys
import subprocess
from PySide6.QtWidgets import (
    QApplication,
    QMainWindow,
    QPushButton,
    QVBoxLayout,
    QWidget,
    QTabWidget,
    QLabel,
    QGridLayout,
)
from PySide6.QtCore import Qt, QThreadPool, QRunnable, Slot
from PySide6.QtGui import QPixmap


class Worker(QRunnable):
    def __init__(self, task_func, on_complete_callback=None):
        super().__init__()
        self.task_func = task_func
        self.on_complete_callback = on_complete_callback

    @Slot()
    def run(self):
        try:
            self.task_func()
        finally:
            if self.on_complete_callback:
                self.on_complete_callback()


class MainApp(QMainWindow):
    xspacing = 10
    yspacing = 10

    def __init__(self):
        super().__init__()
        self.setWindowTitle("Mellanni tools app")
        self.setFixedSize(600, 450)
        self.executor = QThreadPool()

        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        self.main_layout = QVBoxLayout(central_widget)

        # --- LOGO SECTION ---
        self.logo_label = QLabel()
        pixmap = QPixmap("media/mellanni.png")
        self.logo_label.setPixmap(
            pixmap.scaled(
                300,
                80,
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            )
        )
        self.main_layout.addWidget(
            self.logo_label, alignment=Qt.AlignmentFlag.AlignCenter
        )

        # Tab View
        self.tab_view = QTabWidget()
        self.main_layout.addWidget(self.tab_view)

        # Setup Tabs
        self.reports_frame = QWidget()
        self.tools_frame = QWidget()
        self.tab_view.addTab(self.reports_frame, "Reports")
        self.tab_view.addTab(self.tools_frame, "Tools")

        # Layouts for tabs
        self.reports_layout = QGridLayout(self.reports_frame)
        self.tools_layout = QGridLayout(self.tools_frame)

        self.setup_widgets()

        # Update button at bottom
        self.update_button = QPushButton("Update")
        self.update_button.setStyleSheet("background-color: gray; color: white;")
        self.update_button.clicked.connect(self.update_git)
        self.main_layout.addWidget(
            self.update_button, alignment=Qt.AlignmentFlag.AlignCenter
        )

    def setup_widgets(self):
        # --- REPORTS SECTION ---
        self.price_check_button = QPushButton("Price checker")
        self.price_check_button.clicked.connect(
            lambda: self.run_task("price_checker", self.price_check_button)
        )
        self.reports_layout.addWidget(self.price_check_button, 0, 0)

        self.weekly_conversion_button = QPushButton("Weekly conversion")
        self.weekly_conversion_button.clicked.connect(
            lambda: self.run_task("weekly_conversion", self.weekly_conversion_button)
        )
        self.reports_layout.addWidget(self.weekly_conversion_button, 1, 0)

        self.restock_button = QPushButton("Restock")
        self.restock_button.clicked.connect(
            lambda: self.run_task("restock", self.restock_button)
        )
        self.reports_layout.addWidget(self.restock_button, 2, 0)

        # --- TOOLS SECTION (COLUMN 0) ---
        self.coupon_helper_button = QPushButton("Coupon helper")
        self.coupon_helper_button.clicked.connect(
            lambda: self.run_task("coupon_helper", self.coupon_helper_button)
        )
        self.tools_layout.addWidget(self.coupon_helper_button, 0, 0)

        self.title_check_button = QPushButton("Check titles")
        self.title_check_button.clicked.connect(
            lambda: self.run_task("check_titles", self.title_check_button)
        )
        self.tools_layout.addWidget(self.title_check_button, 1, 0)

        self.title_duplicate_check_button = QPushButton("Check duplicates in titles")
        self.title_duplicate_check_button.clicked.connect(
            lambda: self.run_task(
                "title_duplicates_checker", self.title_duplicate_check_button
            )
        )
        self.tools_layout.addWidget(self.title_duplicate_check_button, 2, 0)

        self.flat_file_transfer_button = QPushButton("Transfer to new flat file")
        self.flat_file_transfer_button.clicked.connect(
            lambda: self.run_task(
                "new_template_transfer", self.flat_file_transfer_button
            )
        )
        self.tools_layout.addWidget(self.flat_file_transfer_button, 3, 0)

        self.image_naming_check_button = QPushButton("Check image names")
        self.image_naming_check_button.clicked.connect(
            lambda: self.run_task("color_name_checker", self.image_naming_check_button)
        )
        self.tools_layout.addWidget(self.image_naming_check_button, 4, 0)

        # --- TOOLS SECTION (COLUMN 1) ---
        self.image_rekognition_button = QPushButton("Image Rekognition")
        self.image_rekognition_button.clicked.connect(
            lambda: self.run_task(
                "aws_image_rekognition", self.image_rekognition_button
            )
        )
        self.tools_layout.addWidget(self.image_rekognition_button, 0, 1)

        self.marketplace_promos_button = QPushButton("Marketplace promos")
        self.marketplace_promos_button.clicked.connect(
            lambda: self.run_task("marketplace_promos", self.marketplace_promos_button)
        )
        self.tools_layout.addWidget(self.marketplace_promos_button, 1, 1)

        self.event_sales_button = QPushButton("Event sales")
        self.event_sales_button.clicked.connect(
            lambda: self.run_task("event_sales", self.event_sales_button)
        )
        self.tools_layout.addWidget(self.event_sales_button, 2, 1)

        self.dimensions_button = QPushButton("Pull and compare dimensions")
        self.dimensions_button.clicked.connect(
            lambda: self.run_task("dimensions", self.dimensions_button)
        )
        self.tools_layout.addWidget(self.dimensions_button, 3, 1)

        self.oversize_button = QPushButton("Check AMZ oversize")
        self.oversize_button.clicked.connect(
            lambda: self.run_task("oversize_check", self.oversize_button)
        )
        self.tools_layout.addWidget(self.oversize_button, 4, 1)

        self.bundle_checker_button = QPushButton("Check bundle inventory")
        self.bundle_checker_button.clicked.connect(
            lambda: self.run_task("bundle_checker", self.bundle_checker_button)
        )
        self.tools_layout.addWidget(self.bundle_checker_button, 5, 1)

    def run_task(self, script_name, button, func_name="main"):
        original_text = button.text()
        button.setText("Please wait...")
        button.setEnabled(False)

        def task_logic():
            try:
                # Dynamic import inside the thread
                module = __import__(f"scripts.{script_name}", fromlist=["main"])
                target_function = getattr(module, func_name)
                target_function()
            except Exception as e:
                print(f"Error in {script_name}: {e}")

        def cleanup():
            button.setText(original_text)
            button.setEnabled(True)

        worker = Worker(task_logic, on_complete_callback=cleanup)
        self.executor.start(worker)

    def update_git(self):
        subprocess.call(["git", "restore", "."])
        subprocess.call(["git", "pull", "-f"])


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    window = MainApp()
    window.show()
    sys.exit(app.exec())
