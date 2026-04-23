import pandas as pd
import re

DEBUG = True

def load_data():
    df = pd.read_excel("Задание.xlsx", sheet_name="Лист1")
    df.columns = df.columns.str.strip()
    
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df = df.dropna(subset=["price", "quantity"])
    df["revenue"] = df["price"] * df["quantity"]
    
    def extract_grams(text):
        if not isinstance(text, str):
            return None
        match = re.search(r'(\d+(?:[.,]\d+)?)\s*(гр\.?|г|кг|грамм)', text, re.IGNORECASE)
        if match:
            val = float(match.group(1).replace(',', '.'))
            unit = match.group(2).lower()
            if unit == 'кг':
                return int(val * 1000)
            else:
                return int(val)
        return None
    
    df["grams"] = df["product_name"].apply(extract_grams)
    
    if DEBUG:
        print(f"Всего строк: {len(df)}")
        print(f"Строк с извлечённой граммовкой: {df['grams'].notna().sum()}")
        print("Примеры (product_name -> grams):")
        for name, gram in df[['product_name', 'grams']].dropna().head(10).values:
            print(f"  {name[:60]:60} -> {gram} г")
    
    return df

def market_data(df):
    format_col = 'generaltype_name' if 'generaltype_name' in df.columns else None
    if format_col is None:
        def detect_format(name):
            if not isinstance(name, str): return "Другое"
            n = name.lower()
            if '3в1' in n or 'кофе-микс' in n: return "3в1 / Кофе-микс"
            if 'растворимый' in n: return "Растворимый"
            if 'молотый' in n: return "Молотый"
            if 'зерновой' in n or 'зерно' in n: return "Зерновой"
            if 'капсула' in n: return "Капсулы"
            return "Другое"
        df["detected_format"] = df["product_name"].apply(detect_format)
        format_col = "detected_format"
        print("Внимание: колонка 'generaltype_name' не найдена, используется автоопределение формата.")
    
    brand_col = 'brand_name' if 'brand_name' in df.columns else None
    if brand_col is None:
        brand_col = 'brand_name'  
    
    chain_col = 'magnit_chain' if 'magnit_chain' in df.columns else None
    if chain_col is None:
        chain_col = 'magnit_chain'
    
    taste_col = 'variant_name' if 'variant_name' in df.columns else None
    
    package_col = 'cover_name' if 'cover_name' in df.columns else None
    
    region_col = 'fed_name' if 'fed_name' in df.columns else None
    
    def detect_roast(name):
        if not isinstance(name, str): return "Не указано"
        n = name.lower()
        if 'арабика' in n: return "Арабика"
        if 'робуста' in n: return "Робуста"
        if 'сублимированный' in n: return "Сублимированный"
        if 'гранулированный' in n: return "Гранулированный"
        return "Классический"
    df["roast"] = df["product_name"].apply(detect_roast)
    roast_col = "roast"
    
    df_filtered = df.copy()
    if brand_col in df_filtered.columns:
        df_filtered = df_filtered[
            df_filtered[brand_col].notna() & 
            (df_filtered[brand_col].astype(str).str.strip() != "") &
            (df_filtered[brand_col] != "(Empty)")
        ]
    
    result = {}
    
    fmt_data = df_filtered.groupby(format_col)["quantity"].sum().reset_index()
    fmt_data.columns = ['label', 'value']
    fmt_data = fmt_data.sort_values('value', ascending=False)
    result["fmt_data"] = fmt_data.to_dict(orient="records")
    
    brands_data = df_filtered.groupby(brand_col)["quantity"].sum().reset_index()
    brands_data.columns = ['label', 'value']
    brands_data = brands_data.sort_values('value', ascending=False).head(12)
    result["brands_data"] = brands_data.to_dict(orient="records")
    
    nets_revenue = df.groupby(chain_col)["revenue"].sum().reset_index()
    nets_revenue.columns = ['label', 'value']
    nets_revenue = nets_revenue.sort_values('value', ascending=False).head(12)
    result["nets_revenue"] = nets_revenue.to_dict(orient="records")
    
    nets_qty_data = df.groupby(chain_col)["quantity"].sum().reset_index()
    nets_qty_data.columns = ['label', 'value']
    nets_qty_data = nets_qty_data.sort_values('value', ascending=False).head(12)
    result["nets_qty_data"] = nets_qty_data.to_dict(orient="records")
    
    if taste_col and taste_col in df_filtered.columns:
        flavor_data = df_filtered.groupby(taste_col)["quantity"].sum().reset_index()
        flavor_data.columns = ['label', 'value']
        flavor_data = flavor_data.sort_values('value', ascending=False).head(10)
        result["flavor_data"] = flavor_data.to_dict(orient="records")
    
    if package_col and package_col in df.columns:
        pack_data = df.groupby(package_col)["quantity"].sum().reset_index()
        pack_data.columns = ['label', 'value']
        pack_data = pack_data.sort_values('value', ascending=False).head(8)
        result["pack_data"] = pack_data.to_dict(orient="records")
    
    roast_sorted = df.groupby(roast_col)["quantity"].sum().reset_index()
    roast_sorted.columns = ['label', 'value']
    roast_sorted = roast_sorted.sort_values('value', ascending=False)
    result["roast_sorted"] = roast_sorted.to_dict(orient="records")
    
    if region_col and region_col in df.columns:
        region_data = df.groupby(region_col)["quantity"].sum().reset_index()
        region_data.columns = ['label', 'value']
        region_data = region_data.sort_values('value', ascending=False).head(8)
        result["region_data"] = region_data.to_dict(orient="records")
    
    df_grams = df[df['grams'].notna()].copy()
    if not df_grams.empty:
        def get_format_group(fmt_val):
            if not isinstance(fmt_val, str):
                return None
            f = fmt_val.lower()
            if 'микс' in f or '3в1' in f or f == 'кофе-микс':
                return 'mix'
            if 'растворим' in f:
                return 'sol'
            if 'молот' in f:
                return 'mol'
            if 'зерн' in f:
                return 'grain'
            return None
        
        df_grams['format_group'] = df_grams[format_col].apply(get_format_group)
        
        for group in ['mix', 'sol', 'mol', 'grain']:
            sub = df_grams[df_grams['format_group'] == group]
            if not sub.empty:
                weights = sub.groupby('grams')['quantity'].sum().reset_index()
                weights.columns = ['label', 'value']
                weights = weights.sort_values('value', ascending=False).head(5)
                weights['label'] = weights['label'].astype(int).astype(str) + ' г'
                result[f"{group}_weights"] = weights.to_dict(orient="records")
            else:
                result[f"{group}_weights"] = []
    
    if chain_col in df.columns and brand_col in df.columns and format_col in df.columns:
        top_nets = df.groupby(chain_col)["quantity"].sum().sort_values(ascending=False).head(6).index
        net_sku_data = {}
        for net in top_nets:
            net_df = df[df[chain_col] == net]
            net_brands = net_df[
                net_df[brand_col].notna() & 
                (net_df[brand_col].astype(str).str.strip() != "") &
                (net_df[brand_col] != "(Empty)")
            ]
            if not net_brands.empty:
                brands = net_brands.groupby(brand_col)["quantity"].sum().reset_index()
                brands.columns = ['label', 'value']
                brands = brands.sort_values('value', ascending=False).head(6)
            else:
                brands = pd.DataFrame(columns=['label','value'])
            formats = net_df.groupby(format_col)["quantity"].sum().reset_index()
            formats.columns = ['label', 'value']
            formats = formats.sort_values('value', ascending=False).head(6)
            avg_price = net_df.groupby(format_col)["price"].mean().reset_index()
            avg_price.columns = ['label', 'value']
            avg_price = avg_price.sort_values('value', ascending=False).head(6)
            net_sku_data[net] = {
                "brands": brands.to_dict(orient="records"),
                "formats": formats.to_dict(orient="records"),
                "avg_price": avg_price.to_dict(orient="records")
            }
        result["net_sku_data"] = net_sku_data
    
    return result