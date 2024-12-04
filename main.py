import customtkinter as ctk
from concurrent.futures import ThreadPoolExecutor
from PIL import Image
logo = Image.open('media/mellanni.png')

class MainApp(ctk.CTk):
    xspacing = 10
    yspacing = 10
    def __init__(self):
        super().__init__()
        self.geometry('600x400')
        self.title('Mellanni tools app')
        self.executor = ThreadPoolExecutor()

        self.button_frame = ctk.CTkFrame(self)
        self.button_frame.grid(row=0, column=0, sticky='ew')

        # self.img_frame = ctk.CTkFrame(self)
        # self.img_frame.grid(row=0, column=1, sticky='ew')
        # self.logo = ctk.CTkImage(dark_image=logo, size=(300,100))
        # self.logo_label = ctk.CTkLabel(self.img_frame, image=self.logo, text="")
        # self.logo_label.grid(row=0, column=0)

        self.price_check_button = ctk.CTkButton(self.button_frame, text='Price checker', command=self.call_price_checker)
        self.price_check_button.grid(row=0, column=0, padx=self.xspacing, pady=self.yspacing)

        self.weekly_conversion_button = ctk.CTkButton(self.button_frame, text='Weekly conversion', command=self.call_weekly_conversion)
        self.weekly_conversion_button.grid(row=1, column=0, padx=self.xspacing, pady=self.yspacing)


    def call_price_checker(self):
        from modules import price_checker
        self.after(200, self.destroy())
        price_checker.main()

    def call_weekly_conversion(self):
        from modules import weekly_conversion
        self.after(200, self.destroy())
        weekly_conversion.main()

if __name__ == '__main__':
    app = MainApp()
    app.mainloop()