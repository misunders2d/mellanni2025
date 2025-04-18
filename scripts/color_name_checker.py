from customtkinter import filedialog
from common import user_folder
import os
import pandas as pd

folder = filedialog.askdirectory(title="Select folder with images", initialdir=user_folder)

files = os.listdir(folder)

products = {}
colors = {}
sizes = {}

for file in files:
    product, color, size, _, market, position = os.path.splitext(file)[0].split('_')
    if product not in products:
        products[product]=1
    else:
        products[product]+=1
    if color not in colors:
        colors[color]=1
    else:
        colors[color]+=1
    if size not in sizes:
        sizes[size]=1
    else:
        sizes[size]+=1        
        
df_products = pd.DataFrame.from_dict(products, orient='index', columns=['count']).reset_index().rename(columns={'index':'product'})
df_colors = pd.DataFrame.from_dict(colors, orient='index', columns=['count']).reset_index().rename(columns={'index':'color'})
df_sizes = pd.DataFrame.from_dict(sizes, orient='index', columns=['count']).reset_index().rename(columns={'index':'size'})


total = pd.concat([df_products, df_colors, df_sizes])
total = total[['product', 'size','color', 'count']]
total.to_excel(os.path.join(user_folder, 'images.xlsx'), index = False)
