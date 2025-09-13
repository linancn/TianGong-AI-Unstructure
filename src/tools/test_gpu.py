import torch

for i in range(torch.cuda.device_count()):
    print(f"Device {i}: {torch.cuda.get_device_name(i)}")


import paddle

print(paddle.__version__)
print(paddle.device.get_device())


# 检查PaddlePaddle是否可以访问GPU
print(paddle.is_compiled_with_cuda())

# 检查当前设备的CUDA版本
print(paddle.version.cuda())

# 检查CuDNN版本
print(paddle.version.cudnn())
