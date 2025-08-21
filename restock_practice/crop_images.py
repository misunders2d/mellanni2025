import customtkinter as ctk
import os
from PIL import Image
from tkinter import filedialog

# Змінні для зберігання шляхів та значень обрізки
input_folder = ""
output_folder = ""
selected_files = []

def main():
    # Налаштування основного вікна
    root = ctk.CTk()
    root.title("Image Cropper")
    root.geometry("420x420")


    # Змінні для значень обрізки
    left_val = ctk.StringVar(value="0")
    top_val = ctk.StringVar(value="0")
    right_val = ctk.StringVar(value="0")
    bottom_val = ctk.StringVar(value="0")

    # Функція для вибору вхідної папки
    def select_input_folder():
        global input_folder
        folder = filedialog.askdirectory(title="Select Input Folder")
        if folder:
            input_folder = folder
            input_label.configure(text=f"Input Folder: {os.path.basename(folder)}")
            selected_files.clear()  # Очищаємо список файлів, якщо вибрано папку

    # Функція для вибору вихідної папки
    def select_output_folder():
        global output_folder
        folder = filedialog.askdirectory(title="Select Output Folder")
        if folder:
            output_folder = folder
            output_label.configure(text=f"Output Folder: {os.path.basename(folder)}")

    # Функція для вибору файлів
    def select_files():
        global selected_files, input_folder
        files = filedialog.askopenfilenames(
            title="Select Image Files",
            filetypes=[("Image files", "*.png *.jpg *.jpeg *.bmp *.tiff")]
        )
        if files:
            selected_files = list(files)
            input_folder = ""  # Очищаємо папку, якщо вибрано файли
            input_label.configure(text=f"Selected Files: {len(selected_files)} files")

    # Функція для обрізки зображень
    def crop_images():
        if not output_folder:
            status_label.configure(text="Please select an output folder!")
            return

        try:
            left = int(left_val.get())
            top = int(top_val.get())
            right = int(right_val.get())
            bottom = int(bottom_val.get())

            if selected_files:  # Якщо вибрано файли
                for file_path in selected_files:
                    with Image.open(file_path) as img:
                        width, height = img.size
                        right_adjusted = width - right if right < width else width
                        bottom_adjusted = height - bottom if bottom < height else height
                        cropped_img = img.crop((left, top, right_adjusted, bottom_adjusted))
                        name, ext = os.path.splitext(os.path.basename(file_path))
                        output_path = os.path.join(output_folder, f"{name}{ext}")
                        cropped_img.save(output_path)
                        print(f"Оброблено: {os.path.basename(file_path)} -> {name}{ext}")
            elif input_folder:  # Якщо вибрано папку
                for filename in os.listdir(input_folder):
                    if filename.lower().endswith(('.png', '.jpg', '.jpeg', '.bmp', '.tiff')):
                        input_path = os.path.join(input_folder, filename)
                        with Image.open(input_path) as img:
                            width, height = img.size
                            right_adjusted = width - right if right < width else width
                            bottom_adjusted = height - bottom if bottom < height else height
                            cropped_img = img.crop((left, top, right_adjusted, bottom_adjusted))
                            name, ext = os.path.splitext(filename)
                            output_path = os.path.join(output_folder, f"{name}{ext}")
                            cropped_img.save(output_path)
                            print(f"Оброблено: {filename} -> {name}{ext}")
            else:
                status_label.configure(text="Please select an input folder or files!")
                return

            status_label.configure(text="Image cropping completed!")
            os.startfile(output_folder)  # Відкриваємо output_folder після завершення
        except ValueError:
            status_label.configure(text="Please enter valid numeric values for cropping!")
        except Exception as e:
            status_label.configure(text=f"Error: {str(e)}")

    # Елементи інтерфейсу
    input_label = ctk.CTkLabel(master=root, text="Input: Not selected")
    input_label.pack(pady=10)

    input_button = ctk.CTkButton(master=root, text="Select Input Folder", command=select_input_folder)
    input_button.pack(pady=5)

    files_button = ctk.CTkButton(master=root, text="Select Image Files", command=select_files)
    files_button.pack(pady=5)

    output_label = ctk.CTkLabel(master=root, text="Output Folder: Not selected")
    output_label.pack(pady=10)

    output_button = ctk.CTkButton(master=root, text="Select Output Folder", command=select_output_folder)
    output_button.pack(pady=5)

    crop_button = ctk.CTkButton(master=root, text="Crop Images", command=crop_images)
    crop_button.pack(pady=20)

    status_label = ctk.CTkLabel(master=root, text="Please select folders or files to start", text_color="gray")
    status_label.pack(pady=10)

    # Поля для введення значень обрізки
    ctk.CTkLabel(master=root, text="Crop Values (pixels):").pack(pady=10)
    ctk.CTkLabel(master=root, text="Left:").pack(side=ctk.LEFT, padx=5)
    left_entry = ctk.CTkEntry(master=root, textvariable=left_val, width=50)
    left_entry.pack(side=ctk.LEFT, padx=5)

    ctk.CTkLabel(master=root, text="Top:").pack(side=ctk.LEFT, padx=5)
    top_entry = ctk.CTkEntry(master=root, textvariable=top_val, width=50)
    top_entry.pack(side=ctk.LEFT, padx=5)

    ctk.CTkLabel(master=root, text="Right:").pack(side=ctk.LEFT, padx=5)
    right_entry = ctk.CTkEntry(master=root, textvariable=right_val, width=50)
    right_entry.pack(side=ctk.LEFT, padx=5)

    ctk.CTkLabel(master=root, text="Bottom:").pack(side=ctk.LEFT, padx=5)
    bottom_entry = ctk.CTkEntry(master=root, textvariable=bottom_val, width=50)
    bottom_entry.pack(side=ctk.LEFT, padx=5)

    # Запуск основного циклу

    root.mainloop()

if __name__ == '__main__':
    main()