import pandas as pd
import customtkinter as ctk
import os
from utils import mellanni_modules as mm

from common import user_folder

x = 5
y = 10


def main():
    app = ctk.CTk()
    app.geometry("600x500")

    asin_frame = ctk.CTkFrame(app)
    asin_frame.grid(row=0, column=0, padx=x, pady=y)
    asin_label = ctk.CTkLabel(asin_frame, text="Input ASINs")
    asin_label.grid(row=0, column=0, padx=x, pady=y)
    asins_input = ctk.CTkTextbox(asin_frame)
    asins_input.grid(row=1, column=0, padx=x, pady=y)

    coll_frame = ctk.CTkFrame(app)
    coll_frame.grid(row=0, column=1, padx=x, pady=y)
    coll_label = ctk.CTkLabel(coll_frame, text="Input collections")
    coll_label.grid(row=0, column=0, padx=x, pady=y)
    colls_input = ctk.CTkTextbox(coll_frame)
    colls_input.grid(row=1, column=0, padx=x, pady=y)

    separator_label = ctk.CTkLabel(app, text="Separator to use")
    separator_label.grid(row=1, column=0, padx=x, pady=y)
    separator = ctk.CTkEntry(app, placeholder_text="enter the separator to use")
    separator.insert(0, ";")
    separator.grid(row=1, column=1, padx=x, pady=y)

    group_label = ctk.CTkLabel(app, text="Type the coupon title to append")
    group_label.grid(row=2, column=0, padx=x, pady=y)
    group = ctk.CTkEntry(app, placeholder_text="For example, 1800 Collection...")
    group.grid(row=2, column=1, padx=x, pady=y)

    def run_coupon():
        asins = [x.strip() for x in asins_input.get(0.0, ctk.END).split("\n") if x]
        groups = [x.strip() for x in colls_input.get(0.0, ctk.END).split("\n") if x]
        sep = separator.get()
        title = group.get()
        if len(asins) == len(groups):
            df = pd.DataFrame(list(zip(asins, groups)), columns=["ASIN", "Group"])
            df["Title"] = df["Group"] + " " + title
            pivot = df.pivot_table(
                values="ASIN", index="Title", aggfunc=sep.join
            ).reset_index()
            output = ctk.filedialog.askdirectory(
                title="Select output folder", initialdir=user_folder
            )
            with pd.ExcelWriter(os.path.join(output, "coupon_groups.xlsx")) as writer:
                pivot.to_excel(writer, sheet_name="groups", index=False)
            mm.open_file_folder(output)

    button = ctk.CTkButton(app, text="OK", command=run_coupon)
    button.grid(row=3, column=0, columnspan=2, padx=x, pady=y)

    app.mainloop()


if __name__ == "__main__":
    main()
