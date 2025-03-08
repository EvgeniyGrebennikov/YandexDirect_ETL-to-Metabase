import psycopg2
from datetime import date, datetime


def transform(extracted_data):
    header = next(extracted_data)
    transformed_res = []

    for row in extracted_data:
        try:
            dict_row = {key: value for key, value in zip(header, row)}
            # Проверка строк и преобразование типа данных
            dict_row["Date"] = datetime.strptime(dict_row["Date"], "%Y-%m-%d").date()
            dict_row["Criterion"] = dict_row.get("Criterion", None).split(" -")[0].strip().replace("+", "") if dict_row.get("Criterion", None) != "---autotargeting" else "autotargeting"
            dict_row["AvgImpressionPosition"] = dict_row.get("AvgImpressionPosition", None) if dict_row.get("AvgImpressionPosition", None) != "--" else None
            dict_row["AvgClickPosition"] = dict_row.get("AvgClickPosition", None) if dict_row.get("AvgClickPosition", None) != "--" else None
            dict_row["TargetingCategory"] = dict_row.get("TargetingCategory", None)
            dict_row["BounceRate"] = dict_row.get("BounceRate", None) if dict_row.get("BounceRate", None) != "--" else None
            dict_row["AvgPageviews"] = dict_row.get("AvgPageviews", None) if dict_row.get("AvgPageviews", None) != "--" else None
            dict_row["Conversions_101103598_LYDC"] = int(dict_row.get("Conversions_101103598_LYDC", 0)) if dict_row.get("Conversions_101103598_LYDC", 0) != "--" else 0
            dict_row["Conversions_129037972_LYDC"] = int(dict_row.get("Conversions_129037972_LYDC", 0)) if dict_row.get("Conversions_129037972_LYDC", 0) != "--" else 0
            dict_row["Conversions_235883055_LYDC"] = int(dict_row.get("Conversions_235883055_LYDC", 0)) if dict_row.get("Conversions_235883055_LYDC", 0) != "--" else 0
            dict_row["Conversions_235893048_LYDC"] = int(dict_row.get("Conversions_235893048_LYDC", 0)) if dict_row.get("Conversions_235893048_LYDC", 0) != "--" else 0
            dict_row["Conversions_274250920_LYDC"] = int(dict_row.get("Conversions_274250920_LYDC", 0)) if dict_row.get("Conversions_274250920_LYDC", 0) != "--" else 0
            dict_row["Conversions_283275292_LYDC"] = int(dict_row.get("Conversions_283275292_LYDC", 0)) if dict_row.get("Conversions_283275292_LYDC", 0) != "--" else 0
            dict_row["Age"] = dict_row.get("Age", None).replace("AGE_", "") if dict_row.get("Age", None) != "UNKNOWN" else None
            dict_row["Gender"] = dict_row.get("Gender", None).replace("GENDER_", "") if dict_row.get("Gender", None) != "UNKNOWN" else None
            dict_row["Device"] = dict_row.get("Device", None) if dict_row.get("Device", None) != "UNKNOWN" else None
            dict_row["RlAdjustmentId"] = dict_row.get("RlAdjustmentId", None) if dict_row.get("RlAdjustmentId", None) != "--" else None
            #
            dict_row["Impressions"] = int(dict_row["Impressions"]) if dict_row.get("Impressions", None) is not None and dict_row.get("Impressions", None) != '--' else None

            dict_row["Clicks"] = int(dict_row["Clicks"]) if dict_row.get("Clicks", None) is not None else None
            dict_row["Cost"] = float(dict_row["Cost"]) if dict_row.get("Cost", None) is not None else None
            dict_row["AvgImpressionPosition"] = float(dict_row["AvgImpressionPosition"]) if dict_row.get("AvgImpressionPosition", None) is not None else None
            dict_row["AvgClickPosition"] = float(dict_row["AvgClickPosition"]) if dict_row.get("AvgClickPosition", None) is not None else None
            dict_row["BounceRate"] = float(dict_row["BounceRate"]) if dict_row.get("BounceRate", None) is not None else None
            dict_row["AvgPageviews"] = float(dict_row["AvgPageviews"]) if dict_row.get("AvgPageviews", None) is not None else None

            transformed_res.append(tuple(dict_row.values()))

        except Exception as err:
            print(f"Ошибка при преобразовании типа данных {err}. Осуществлен пропуск строки: {dict_row}")

    return (header, transformed_res)


