import customtkinter as ctk
from customtkinter import filedialog

class PopupError(ctk.CTk):
    def __init__(self, message):
        super().__init__()
        self.geometry('300x100')
        self.title("Error")
        self.message = message

        self.label = ctk.CTkLabel(self, text=self.message)
        self.label.pack(pady=10)

        self.button = ctk.CTkButton(self, text='Error', command=self.ok_button_click, fg_color='red', text_color='white')
        self.button.pack(pady=10)

        self.update_idletasks()
        required_width = max(self.label.winfo_reqwidth(), self.winfo_reqwidth()) + 20
        required_height = self.label.winfo_reqheight() + self.button.winfo_reqheight() + 40
        self.geometry(f"{required_width}x{required_height}")

        self.mainloop()

    def ok_button_click(self):
        self.destroy() 

class PopupWarning(ctk.CTk):
    def __init__(self, message):
        super().__init__()
        self.geometry('300x100')
        self.title("Warning")
        self.message = message

        self.label = ctk.CTkLabel(self, text=self.message)
        self.label.pack(pady=10)

        self.button = ctk.CTkButton(self, text='OK', command=self.ok_button_click, fg_color='yellow', text_color='black')
        self.button.pack(pady=10)

        self.update_idletasks()
        required_width = max(self.label.winfo_reqwidth(), self.winfo_reqwidth()) + 20
        required_height = self.label.winfo_reqheight() + self.button.winfo_reqheight() + 40
        self.geometry(f"{required_width}x{required_height}")

        self.mainloop()

    def ok_button_click(self):
        self.destroy()

class PopupYesNo(ctk.CTk):
    def __init__(self, message, title="Question"):
        super().__init__()
        self.geometry('300x100')
        self.title(title)
        self.message = message

        self.label = ctk.CTkLabel(self, text=self.message)
        self.label.pack(pady=10)

        self.button_yes = ctk.CTkButton(self, text='Yes', command=lambda: self.button_click(True))
        self.button_yes.pack(pady=10, side='left')

        self.button_no = ctk.CTkButton(self, text='No', command=lambda: self.button_click(False))
        self.button_no.pack(pady=10, side='right')

        self.update_idletasks()
        required_width = max(self.label.winfo_reqwidth(), self.winfo_reqwidth()) + 20
        required_height = self.label.winfo_reqheight() + self.button_yes.winfo_reqheight( )+ self.button_no.winfo_reqheight() + 40
        self.geometry(f"{required_width}x{required_height}")

        self.mainloop()

    def button_click(self, value):
        self.result = value
        self.quit()
