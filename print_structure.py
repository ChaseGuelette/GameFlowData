import os

def list_files(startpath):
    # Folders to ignore
    ignore = {'.git', '.venv', 'venv', '__pycache__', '.idea', '.vscode'}
    
    for root, dirs, files in os.walk(startpath):
        dirs[:] = [d for d in dirs if d not in ignore]
        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * (level)
        print('{}{}/'.format(indent, os.path.basename(root)))
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            if not f.endswith('.pyc'): # Ignore compiled python
                print('{}{}'.format(subindent, f))

if __name__ == '__main__':
    list_files('.')