# Encoding solution source: https://codereview.stackexchange.com/questions/202928/convert-to-utf-8-all-files-in-a-directory
import chardet

def change_encoding(filename):
    with open(filename, 'rb') as f:
        content_bytes = f.read()
    detected = chardet.detect(content_bytes)
    encoding = detected['encoding']
    content_text = content_bytes.decode(encoding)
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(content_text)


path = './books_encoded/'
for book in listdir(path):
    print(book)
    change_encoding(path+book)
print('ready')