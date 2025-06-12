import os
import json
import pandas as pd
from src.crawl_data.main_get_data import hanoi_gold_data, hcm_gold_data


def standardize_columns(df):
    """Chuẩn hóa tên cột"""
    column_mapping = {
        "Loại vàng": "LOAIVANG",
        "Giá mua": "GIAMUA",
        "Giá bán": "GIABAN",
        "Date": "NGAY",
        "Thành Phố": "THANHPHO"
    }

    # Đổi tên cột nếu có
    df_copy = df.copy()
    for old_col, new_col in column_mapping.items():
        if old_col in df_copy.columns:
            df_copy = df_copy.rename(columns={old_col: new_col})

    # Đảm bảo có đủ các cột cần thiết
    required_cols = ["LOAIVANG", "GIAMUA", "GIABAN", "NGAY", "THANHPHO"]
    for col in required_cols:
        if col not in df_copy.columns:
            df_copy[col] = ""

    return df_copy[required_cols]


def main():
    # Chuẩn hóa dữ liệu mới
    print("Chuẩn hóa dữ liệu mới...")
    hcm_standardized = standardize_columns(hcm_gold_data)
    hanoi_standardized = standardize_columns(hanoi_gold_data)

    # Gộp dữ liệu mới
    new_data = pd.concat([hcm_standardized, hanoi_standardized], ignore_index=True)
    print(f"Dữ liệu mới: {len(new_data)} dòng")

    # Tạo thư mục nếu chưa tồn tại
    folder_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "temp_json_data")
    os.makedirs(folder_path, exist_ok=True)

    # Đường dẫn đến file JSON
    json_file = os.path.join(folder_path, "gold_price_data.json")

    # Xử lý dữ liệu cũ
    if os.path.exists(json_file):
        print("Đọc dữ liệu cũ...")
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                old_data_json = json.load(f)

            if old_data_json:  # Kiểm tra file không rỗng
                old_data = pd.DataFrame(old_data_json)
                old_data = standardize_columns(old_data)
                print(f"Dữ liệu cũ: {len(old_data)} dòng")

                # Gộp dữ liệu cũ và mới
                combined_df = pd.concat([old_data, new_data], ignore_index=True)
            else:
                print("File cũ rỗng, chỉ sử dụng dữ liệu mới")
                combined_df = new_data
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Lỗi đọc file cũ: {e}. Sử dụng dữ liệu mới")
            combined_df = new_data
    else:
        print("Không có file cũ, tạo mới")
        combined_df = new_data

    print(f"Tổng dữ liệu trước xử lý: {len(combined_df)} dòng")

    # Loại bỏ dữ liệu null trước khi xử lý trùng lặp
    combined_df = combined_df.dropna(subset=["LOAIVANG", "GIAMUA", "GIABAN", "NGAY", "THANHPHO"])
    print(f"Sau khi loại bỏ null: {len(combined_df)} dòng")

    # Xử lý trùng lặp (giữ bản ghi mới nhất)
    initial_count = len(combined_df)
    combined_df = combined_df.drop_duplicates(
        subset=["LOAIVANG", "GIAMUA", "GIABAN", "NGAY", "THANHPHO"],
        keep='last'  # Giữ bản ghi cuối (mới nhất)
    )
    removed_duplicates = initial_count - len(combined_df)
    print(f"Đã loại bỏ {removed_duplicates} bản ghi trùng lặp")
    print(f"Dữ liệu cuối cùng: {len(combined_df)} dòng")

    # Kiểm tra xem có dữ liệu để ghi không
    if len(combined_df) == 0:
        print("Không có dữ liệu để ghi!")
        return

    # Ghi dữ liệu
    try:
        # Sắp xếp theo ngày để dữ liệu có thứ tự
        if 'NGAY' in combined_df.columns:
            combined_df = combined_df.sort_values('NGAY', ascending=False)

        combined_df.to_json(json_file, orient="records", force_ascii=False, indent=4)
        print(f"✅ Dữ liệu đã được lưu thành công tại: {json_file}")
        print(f"📊 Tổng số bản ghi: {len(combined_df)}")

        # Hiển thị thống kê
        if 'THANHPHO' in combined_df.columns:
            city_stats = combined_df['THANHPHO'].value_counts()
            print("📍 Thống kê theo thành phố:")
            for city, count in city_stats.items():
                print(f"   - {city}: {count} bản ghi")

    except Exception as e:
        print(f"❌ Lỗi khi ghi JSON: {e}")

        # Phương pháp dự phòng
        print("🔄 Thử phương pháp dự phòng...")
        try:
            data_dict = []
            for _, row in combined_df.iterrows():
                record = {
                    "LOAIVANG": str(row["LOAIVANG"]) if pd.notna(row["LOAIVANG"]) else "",
                    "GIAMUA": str(row["GIAMUA"]) if pd.notna(row["GIAMUA"]) else "",
                    "GIABAN": str(row["GIABAN"]) if pd.notna(row["GIABAN"]) else "",
                    "NGAY": str(row["NGAY"]) if pd.notna(row["NGAY"]) else "",
                    "THANHPHO": str(row["THANHPHO"]) if pd.notna(row["THANHPHO"]) else ""
                }
                data_dict.append(record)

            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(data_dict, f, ensure_ascii=False, indent=4)
            print(f"✅ Đã ghi dữ liệu bằng phương pháp dự phòng tại: {json_file}")
        except Exception as backup_error:
            print(f"❌ Phương pháp dự phòng cũng thất bại: {backup_error}")


if __name__ == "__main__":
    main()