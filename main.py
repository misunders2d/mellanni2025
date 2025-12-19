import customtkinter as ctk
from concurrent.futures import ThreadPoolExecutor
from PIL import Image

logo = Image.open("media/mellanni.png")


class MainApp(ctk.CTk):
    xspacing = 10
    yspacing = 10

    def __init__(self):
        super().__init__()
        self.geometry("600x400")
        self.title("Mellanni tools app")
        self.executor = ThreadPoolExecutor()

        self.tab_view = ctk.CTkTabview(self, width=600, height=300, anchor="center")
        self.tab_view.grid(row=0, column=0, sticky="ew")
        self.reports_frame = self.tab_view.add("Reports")
        self.tools_frame = self.tab_view.add("Tools")

        # reports section
        self.price_check_button = ctk.CTkButton(
            self.reports_frame, text="Price checker", command=self.call_price_checker
        )
        self.price_check_button.grid(
            row=0, column=0, padx=self.xspacing, pady=self.yspacing
        )

        self.weekly_conversion_button = ctk.CTkButton(
            self.reports_frame,
            text="Weekly conversion",
            command=self.call_weekly_conversion,
        )
        self.weekly_conversion_button.grid(
            row=1, column=0, padx=self.xspacing, pady=self.yspacing
        )

        self.restock_button = ctk.CTkButton(
            self.reports_frame, text="Restock", command=self.call_restock
        )
        self.restock_button.grid(
            row=2, column=0, padx=self.xspacing, pady=self.yspacing
        )

        # tools section
        self.coupon_helper_button = ctk.CTkButton(
            self.tools_frame, text="Coupon helper", command=self.call_coupon_helper
        )
        self.coupon_helper_button.grid(
            row=0, column=0, padx=self.xspacing, pady=self.yspacing
        )

        self.title_check_button = ctk.CTkButton(
            self.tools_frame, text="Check titles", command=self.call_check_titles
        )
        self.title_check_button.grid(
            row=1, column=0, padx=self.xspacing, pady=self.yspacing
        )

        self.title_duplicate_check_button = ctk.CTkButton(
            self.tools_frame,
            text="Check duplicates in titles",
            command=self.call_check_title_duplicates,
        )
        self.title_duplicate_check_button.grid(
            row=2, column=0, padx=self.xspacing, pady=self.yspacing
        )

        self.flat_file_transfer_button = ctk.CTkButton(
            self.tools_frame,
            text="Transfer to new flat file",
            command=self.call_flat_file_transfer,
        )
        self.flat_file_transfer_button.grid(
            row=3, column=0, padx=self.xspacing, pady=self.yspacing
        )

        self.image_naming_check_button = ctk.CTkButton(
            self.tools_frame,
            text="Check image names",
            command=self.call_image_naming_check,
        )
        self.image_naming_check_button.grid(
            row=4, column=0, padx=self.xspacing, pady=self.yspacing
        )

        self.image_rekognition_button = ctk.CTkButton(
            self.tools_frame,
            text="Image Rekognition",
            command=self.call_image_rekognition,
        )
        self.image_rekognition_button.grid(
            row=0, column=1, padx=self.xspacing, pady=self.yspacing
        )

        self.marketplace_promos_button = ctk.CTkButton(
            self.tools_frame,
            text="Marketplace promos",
            command=self.call_marketplace_promos,
        )

        self.marketplace_promos_button.grid(
            row=1, column=1, padx=self.xspacing, pady=self.yspacing
        )
        self.event_sales_button = ctk.CTkButton(
            self.tools_frame,
            text="Event sales",
            command=self.call_event_sales,
        )
        self.event_sales_button.grid(
            row=2, column=1, padx=self.xspacing, pady=self.yspacing
        )

        self.dimensions_button = ctk.CTkButton(
            self.tools_frame,
            text="Pull and compare dimensions",
            command=self.call_dimensions,
        )
        self.dimensions_button.grid(
            row=2, column=1, padx=self.xspacing, pady=self.yspacing
        )

        self.oversize_button = ctk.CTkButton(
            self.tools_frame,
            text="Check AMZ oversize",
            command=self.call_oversize,
        )
        self.oversize_button.grid(
            row=3, column=1, padx=self.xspacing, pady=self.yspacing
        )

        # bottom section
        self.update_button = ctk.CTkButton(
            self, text="Update", fg_color="gray", command=self.update
        )
        self.update_button.grid(row=3, column=0, pady=20)

    def update(self):
        import subprocess
        import os
        venv_folder = '.venv' if '.venv' in os.listdir(os.getcwd()) else 'venv'

        subprocess.call(["git", "restore", "."])
        subprocess.call(["git", "pull", "-f"])
        subprocess.call([fr"{venv_folder}\Scripts\activate"])
        subprocess.call(["pip", "install", "-r", "requirements.txt", "--upgrade"])
        subprocess.call(['uv','sync'])

    def call_image_rekognition(self):
        from scripts import aws_image_rekognition

        # self.after(200, self.destroy)
        aws_image_rekognition.main()

    def call_image_naming_check(self):
        from scripts import color_name_checker

        # self.after(200, self.destroy)
        color_name_checker.main()

    def call_flat_file_transfer(self):
        from scripts import new_template_transfer

        # self.after(200, self.destroy)
        new_template_transfer.main()

    def call_restock(self):
        from scripts import restock

        # self.after(200, self.destroy)
        restock.main()

    def call_price_checker(self):
        from scripts import price_checker

        # self.after(200, self.destroy)
        price_checker.main()

    def call_weekly_conversion(self):
        from scripts import weekly_conversion

        # self.after(200, self.destroy)
        weekly_conversion.main()

    def call_coupon_helper(self):
        from scripts import coupon_helper

        # self.after(200, self.destroy)
        coupon_helper.main()

    def call_check_titles(self):
        from scripts import check_titles

        # self.after(200, self.destroy)
        check_titles.main()

    def call_check_title_duplicates(self):
        from scripts import title_duplicates_checker

        # self.after(200, self.destroy)
        title_duplicates_checker.main()
        # title_duplicates_checker.run_custom_file()

    def call_marketplace_promos(self):
        from scripts import marketplace_promos

        # self.after(200, self.destroy)
        marketplace_promos.main()

    def call_event_sales(self):
        from scripts import event_sales

        # self.after(200, self.destroy)
        self.executor.submit(event_sales.main)

    def call_dimensions(self):
        from scripts import dimensions

        # self.after(200, self.destroy)
        self.executor.submit(dimensions.main)

    def call_oversize(self):
        from scripts import oversize_check
        self.executor.submit(oversize_check.main)


if __name__ == "__main__":
    app = MainApp()
    app.mainloop()

# another change2
