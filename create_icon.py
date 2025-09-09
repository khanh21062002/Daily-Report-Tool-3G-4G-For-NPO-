"""
Script tạo icon đơn giản cho ứng dụng
"""

from PIL import Image, ImageDraw, ImageFont
import os


def create_app_icon():
    """Tạo icon cho ứng dụng"""

    # Tạo icon 256x256
    size = 256
    img = Image.new('RGBA', (size, size), (0, 0, 0, 0))
    draw = ImageDraw.Draw(img)

    # Vẽ background gradient
    for i in range(size):
        color = (74, 144, 226, 255)  # Blue gradient
        draw.rectangle([0, i, size, i + 1], fill=color)

    # Vẽ biểu tượng báo cáo
    margin = 40

    # Vẽ document
    doc_color = (255, 255, 255, 255)
    draw.rectangle([margin, margin, size - margin, size - margin], fill=doc_color, outline=(0, 0, 0, 255), width=3)

    # Vẽ lines trên document
    line_color = (100, 100, 100, 255)
    for i in range(4):
        y = margin + 50 + (i * 30)
        draw.rectangle([margin + 20, y, size - margin - 20, y + 5], fill=line_color)

    # Vẽ chart
    chart_color = (46, 204, 113, 255)
    chart_x = margin + 20
    chart_y = size - margin - 80
    chart_w = size - 2 * margin - 40
    chart_h = 60

    # Chart bars
    bar_width = chart_w // 5
    for i in range(5):
        bar_height = 20 + (i * 8)
        x = chart_x + (i * bar_width) + 5
        y = chart_y + chart_h - bar_height
        draw.rectangle([x, y, x + bar_width - 10, chart_y + chart_h], fill=chart_color)

    # Lưu icon
    img.save('icon.ico')
    print("✅ Created icon.ico")

    return True


if __name__ == "__main__":
    create_app_icon()