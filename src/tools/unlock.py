import os
import sys

import pikepdf


def unlock_file(file):
    pdf = pikepdf.open(file, allow_overwriting_input=True)
    pdf.save(file)


def unlock_directory(folder="new"):
    os.chdir(folder)
    filelist = os.listdir()
    for file in filelist:
        if os.path.splitext(file)[1] == ".pdf":
            unlock_file(file)


if __name__ == "__main__":
    if len(sys.argv) == 1:
        unlock_directory()
    else:
        target = sys.argv[1]
        if os.path.isdir(target):
            unlock_directory(target)
        else:
            unlock_file(target)

    print("done")
