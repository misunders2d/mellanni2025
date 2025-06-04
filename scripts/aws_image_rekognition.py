import boto3
import os
from PIL import Image, ImageDraw, ImageFont
from customtkinter import filedialog

from utils import mellanni_modules as mm
from common import user_folder

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '../.env'))

AWS_ACCESS_KEY=os.environ.get('AWS_ACCESS_KEY')
AWS_SECRET=os.environ.get('AWS_SECRET')
REGION_NAME=os.environ.get('REGION_NAME')

if any([AWS_ACCESS_KEY is None, AWS_SECRET is None, REGION_NAME is None]):
    raise ValueError("Please set AWS_ACCESS_KEY, AWS_SECRET, and REGION_NAME in your .env file!")

img_path = filedialog.askdirectory(title='Select folder with images', initialdir=user_folder)
if not img_path:
    raise ValueError("No folder selected. Please select a folder with images.")
else:
    out_folder = os.path.join(img_path, 'tagged_images')
    os.makedirs(out_folder, exist_ok=True)

def get_file_paths(img_path):
    files = os.listdir(img_path)
    file_paths = [os.path.join(img_path, file) for file in files if any(['.png' in file.lower(),'.jpg' in file.lower()])]
    return file_paths


def detect_labels(photo, client):
    print(f'Reading file {photo}')
    with open(photo, 'rb') as img:
        response = client.detect_labels(Image={'Bytes': img.read()}, MaxLabels = 10)
    image = Image.open(photo)
    
    imgWidth, imgHeight = image.size
    draw = ImageDraw.Draw(image)


    #get labels
    print()
    items = []
    for label in response['Labels']:
        row = {}
        row['Label name'] = label['Name']
        print("Label: " + label['Name'])
        row['Confidence'] = label['Confidence']
        print("Confidence: " + str(label['Confidence']))
        print("Instances:")
        row['BoundingBoxes'] = {'BoundingBox':[]}
        for instance in label['Instances']:
            # print(" Bounding box")
            row['BoundingBoxes']['BoundingBox'].append(instance['BoundingBox'])
            print(" Top: " + str(instance['BoundingBox']['Top']))
            print(" Left: " + str(instance['BoundingBox']['Left']))
            print(" Width: " + str(instance['BoundingBox']['Width']))
            print(" Height: " + str(instance['BoundingBox']['Height']))
            print(" Confidence: " + str(instance['Confidence']))
            print()
            
    
        row['Categories'] = [list(x.values()) for x in label['Categories']]


        items.append(row)
        
    #draw bounding boxes
    labels_with_bounding_boxes = [x for x in items if x['BoundingBoxes']['BoundingBox'] != []]
    all_boxes = []
    all_labels = []
    for objects in labels_with_bounding_boxes:#[::-1]:
        label_name = objects['Label name']
        confidence = objects['Confidence']
        for box in objects['BoundingBoxes']['BoundingBox']:
            if box in all_boxes:
                break
            left = imgWidth * box['Left']
            top = imgHeight * box['Top']
            width = imgWidth * box['Width']
            height = imgHeight * box['Height']
            draw.rectangle([left,top, left + width, top + height], outline='#00d400')
            draw.text((left,top), f"{label_name}: {int(confidence)}%", (255,0,0), font = ImageFont.truetype('arial',40), align ="left")
            all_boxes.append(box)
            all_labels.append({label_name:int(confidence)})
            
            
        
    image.save(os.path.join(out_folder,os.path.basename(photo)))
    

    if "ImageProperties" in str(response):
        print("Background:")
        print(response["ImageProperties"]["Background"])
        print()
        print("Foreground:")
        print(response["ImageProperties"]["Foreground"])
        print()
        print("Quality:")
        print(response["ImageProperties"]["Quality"])
        print()
    
    return len(response['Labels'])


def detect_and_draw_labels(photo): #updated process
    rekognition = boto3.client(
        'rekognition',
        aws_access_key_id=AWS_ACCESS_KEY,
        region_name=REGION_NAME,
        aws_secret_access_key=AWS_SECRET)

    with open(photo, 'rb') as image_file:
        image_bytes = image_file.read()

    response = rekognition.detect_labels(
        Image={'Bytes': image_bytes},
        MaxLabels=10,
        MinConfidence=0.5
    )

    image = Image.open(photo)
    draw = ImageDraw.Draw(image)

    try:
        font = ImageFont.truetype("arial.ttf", 25)
    except:
        font = None  # No font, text may not render correctly

    img_width, img_height = image.size

    for label in response['Labels']:
        for instance in label.get('Instances', []):
            bbox = instance['BoundingBox']
            # Calculate pixel coordinates for bounding box
            left = img_width * bbox['Left']
            top = img_height * bbox['Top']
            width = img_width * bbox['Width']
            height = img_height * bbox['Height']
            right = left + width
            bottom = top + height

            # Draw rectangle
            draw.rectangle(
                [left, top, right, bottom],
                outline='red',
                width=2
            )

            # Draw label text
            label_text = f"{label['Name']} ({instance['Confidence']:.1f}%)"
            text_x = left
            text_y = top - 20 if top > 20 else top + height + 5
            draw.text(
                (text_x, text_y),
                label_text,
                fill='red',
                font=font
            )

    # Save the annotated image
    output_path = os.path.join(out_folder,os.path.basename(photo))
    image.save(output_path, quality=95)
    print(f"Annotated image saved as {output_path}")

def main():

    file_paths = get_file_paths(img_path)
    for photo in file_paths:
        # label_count = detect_labels(photo, client, img_path)
        detect_and_draw_labels(photo)
    
    mm.open_file_folder(out_folder)

if __name__ == "__main__":
    main()
