import zipfile
import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import cv2
import rasterio
import cv2
import numpy as np

# image_path = './sample/Sample/01.원천데이터/R20220712A19B0128.tif'
# path = './sample/Sample/01.원천데이터/R20220712A19B0128.tif'

# with rasterio.open(path) as src:
#     img = src.read(1)  # (bands, H, W)

# # 정규화
# vis = cv2.normalize(img, None, 0, 255, cv2.NORM_MINMAX)
# vis = vis.astype(np.uint8)
# color = cv2.applyColorMap(vis,cv2.COLORMAP_JET)

# cv2.imshow('Color', color)
# cv2.waitKey(0)
# cv2.destroyAllWindows()


# 압축 파일 경로
zip_file_path = './108.벼 생육이상 인식 데이터/Training/01.원천데이터/TS_1.zip'
 # 압축 해제할 폴더 경로
extract_path = './벼 생육이상 인식 데이터/train/01.원천데이터/TS-1'

 # ZIP 파일 압축 해제
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(extract_path)

print(f"{zip_file_path} 압축 해제가 완료되었습니다.")