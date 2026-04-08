with open("preprocess.py", "r") as f:
    text = f.read()
if "import sys" not in text:
    text = text.replace("import os", "import os\nimport sys", 1)
    with open("preprocess.py", "w") as f:
        f.write(text)
