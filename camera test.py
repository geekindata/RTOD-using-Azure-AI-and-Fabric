import cv2

def list_available_cameras(max_index=10):
    available_cameras = []
    
    for index in range(max_index):
        cap = cv2.VideoCapture(index, cv2.CAP_DSHOW)  # Try DirectShow (for Windows)
        if cap.isOpened():
            available_cameras.append(index)
            cap.release()
    
    return available_cameras

cameras = list_available_cameras()
if cameras:
    print(f"Available cameras: {cameras}")
else:
    print("No available cameras found.")
