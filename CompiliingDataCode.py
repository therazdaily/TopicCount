import os
import pandas as pd
import re

# Define the folder containing CSV files
downloads_folder = "/Users/mobinariazi/Downloads/Telegram_Scraped_Data"  # Update this path
compiled_csv_path = os.path.join(downloads_folder, "Telegram_Data_Compiled.csv")

# Get all CSV files in the folder
csv_files = [f for f in os.listdir(downloads_folder) if f.endswith(".csv.csv")]

# Keyword Categories
keyword_categories = {
    "Industries_Businesses": [
        "فیلترشکن", "اینفلوئنسر", "بلاگر", "کافه", "رستوران", "کلینیک", "شکن",
        "اینستاگرام", "سالن", "آرایشگاه", "آرایشگر", "سرقت", "قاچاق",
        "پیرایشگاه","تشریفات", "صنفی", "دی جی", "تبلیغات", "باشگاه", "جیم", "بازار", "تبلیغات نامتعارف"
    ],
    "Enforcement_Terms": [
        "پلمپ", "جریمه", "پلمب", "بست", "تعطیل", "برخورد", "حبس", "دستگیر", "هشدار",
        "اجرای", "مسدود", "محکومیت", "اخطاریه", "محروم", "مهروموم"
    ],
    "Morality_Terms": [
        "هنجار", "کشف حجاب", "حجاب", "بی حجابی", "نامناسب",
        "استعمار", "متخلف", "عفاف", "مبتذل", "فرهنگ", "غرب"
    ],
    "Safety_Terms": [
        "بهداشت", "سلامتی", "حفاظت", "امنیت", "جلوگیری", "پاکسازی", "خطر"
    ],
    "Demographic_Terms": [
        "جوان", "دختر", "کودکان", "خانواده", "پسر", "پدر و مادر", "والدین",
        "مردان", "زنان"
    ],
    "Operative_Terms": [
        "معاونت", "مبارزه", "رصد", "نظارت", "بررسی", "غیر مجاز", "مجوز", "قضایی",
        "طرح نور", "کشف", "رسیدگی قضایی", "مجاز"
    ]
}

# Flatten keyword list
all_keywords = [kw for cat in keyword_categories.values() for kw in cat]

# Initialize lists and dictionaries
df_list = []
file_view_counts = {}  # Store total views per file
skipped_files = []  # Track empty or corrupted files

# Compile all CSVs first
for file in csv_files:
    file_path = os.path.join(downloads_folder, file)
    try:
        # Read CSV with encoding flexibility
        try:
            df = pd.read_csv(file_path, encoding="utf-8-sig", low_memory=False)
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding="utf-16", low_memory=False)

        df["Source File"] = file  # Track file source

        # Ensure "Views" column exists and convert to numeric
        df["Views"] = pd.to_numeric(df.get("Views", 0), errors="coerce").fillna(0).astype(int)
        file_view_counts[file] = df["Views"].sum()

        # Ensure "Message" column exists and convert to string
        df["Message"] = df.get("Message", "").fillna("No Text").astype(str)

        df_list.append(df)  # Append to the list
    except Exception as e:
        print(f"Error loading {file}: {e}")
        skipped_files.append(file)

# Merge all CSVs into one DataFrame
if df_list:
    compiled_df = pd.concat(df_list, ignore_index=True)

    # Add "New Number" column if not already present
    if "New Number" not in compiled_df.columns:
        compiled_df.insert(1, "New Number", range(1, len(compiled_df) + 1))

    # Compute overall totals
    total_messages = len(compiled_df)
    total_views = compiled_df["Views"].sum()

    # Create keyword columns within categories (set all to 0 initially)
    for category, keywords in keyword_categories.items():
        for keyword in keywords:
            compiled_df[keyword] = 0

    # Efficient keyword counting using regex vectorization
    def count_keywords(text):
        return {kw: len(re.findall(rf"\b{re.escape(kw)}\b", text, re.IGNORECASE)) for kw in all_keywords}

    keyword_counts = compiled_df["Message"].apply(count_keywords).apply(pd.Series)
    compiled_df.update(keyword_counts)  # Efficiently update DataFrame with keyword counts

    # Print total occurrences for each keyword category
    print("\nTotal occurrences categorized:")
    for category, keywords in keyword_categories.items():
        print(f"\n**{category.replace('_', ' ')}**")
        for keyword in keywords:
            total_keyword_count = compiled_df[keyword].sum()
            print(f"   🔑 {keyword}: {total_keyword_count}")

    # Save the compiled DataFrame
    compiled_df.to_csv(compiled_csv_path, index=False, encoding="utf-8-sig")

    print(f"\nSuccessfully compiled {total_messages} messages from {len(csv_files) - len(skipped_files)} valid CSVs.")
    print(f"Total Views: {total_views}")
    for file, views in list(file_view_counts.items())[:30]:  # Show first 30 files
        print(f"📂 {file}: {views} views")
    print(f"💾 Saved to {compiled_csv_path}")

    if skipped_files:
        print(f"Skipped {len(skipped_files)} files (empty or corrupted):")
        for f in skipped_files[:10]:  # Show first 10 skipped files
            print(f"   - {f}")
else:
    print("No valid CSV files found in the directory.")
