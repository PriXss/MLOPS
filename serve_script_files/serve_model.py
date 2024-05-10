import os

directory = os.environ.get('model_name')
print(f'changing directory to {directory}')
os.system(f'cd {directory}')
os.system(f'ludwig serve -m {directory}')
