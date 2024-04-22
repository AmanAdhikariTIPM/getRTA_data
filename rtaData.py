import mysql.connector
import pandas as pd
import re


def get_RTA_data(data_types): 
    # Establish database connection and execute query
    db = mysql.connector.connect(
        host="192.168.1.252",
        user="root",
        passwd="helloworld86",
        database="MAKsTIPMInventory"
    )
    query = """
    SELECT
        r.rta_id, r.date_created, r.message, r.assigned_to AS rta_assigned_to,
        r.qc, r.original_order_number,
        t1.end_date AS task1_end_date, t1.task_notes AS task1_notes,
        t2.end_date AS task2_end_date, t2.task_notes AS task2_notes,
        t3.end_date AS task3_end_date, t3.task_notes AS task3_notes
    FROM rta_tipm r
    LEFT JOIN task1 t1 ON r.rta_id = t1.order_number
    LEFT JOIN task2 t2 ON r.rta_id = t2.order_number
    LEFT JOIN task3 t3 ON r.rta_id = t3.order_number
    WHERE r.qc = 'Reject';
    """
    cursor = db.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()
    db.close()
    column_names = [
        "rta_id", "date_created", "message", "rta_assigned_to",
        "qc", "original_order_number", "task1_end_date", "task1_notes",
        "task2_end_date", "task2_notes", "task3_end_date", "task3_notes"
    ]
    df = pd.DataFrame(rows, columns=column_names)

    # Define keywords and error codes
    keywords = ['Fuses', 'Reprogram', 'Logic Board', 'Fuel Pump', 'Relay', 'Board']
    error_codes = ["2-24", "2-22", "3-3", "3-10", "3-11", "3-19", "3-22", "4-7", "5-12", "5-26", "k6", "g3", "g5", "G19", "H21", "C1-3", "C3-9", "C3-20", "C4-7", "C5-17", "C6-12", "h9", "h17", "H21", "6-28", "4-7", "2-24", "G19"]
    task_columns = ['task1_notes', 'task2_notes', 'task3_notes']

    # Analyze data
    task_keyword_counts = count_keywords(df, keywords, task_columns)
    error_code_counts = count_error_codes(df, error_codes, task_columns)
    keyword_percentages = get_percentage(task_keyword_counts)
    total_rejects = len(df)

    # Compile results based on requested data types
    results = {}
    for data_type in data_types:
        if data_type == "Percentage":
            results[data_type] = keyword_percentages
        elif data_type == "err_codes":
            results[data_type] = error_code_counts
        elif data_type == "Occurrences":
            results[data_type] = task_keyword_counts
        elif data_type == "Total":
            results[data_type] = total_rejects
    
    # Format output
    for key, value in results.items():
        print(f"\n{key} Data:\n{'-' * 30}")
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                print(f"{subkey}: {subvalue}")
        else:
            print(value)

    return results

# Helper functions
def count_keywords(df, keywords, task_columns):
    task_keyword_counts = {}
    for task_column in task_columns:
        keyword_counts = {keyword: 0 for keyword in keywords}
        for note in df[task_column].dropna():
            note_lower = note.lower()
            for keyword in keywords:
                if keyword.lower() in note_lower:
                    keyword_counts[keyword] += 1
        task_keyword_counts[task_column] = keyword_counts
    return task_keyword_counts

def count_error_codes(df, error_codes, task_columns):
    pattern = re.compile(r'\b(?:' + '|'.join(map(re.escape, error_codes)) + r')\b', re.IGNORECASE)
    error_code_counts = {code.upper(): 0 for code in error_codes}
    for column in task_columns:
        for note in df[column].dropna():
            matches = pattern.findall(note)
            for match in matches:
                error_code_counts[match.upper()] += 1
    return error_code_counts

def get_percentage(task_keyword_counts):
    aggregated_keyword_counts = {}
    for counts_dict in task_keyword_counts.values():
        for keyword, count in counts_dict.items():
            if keyword not in aggregated_keyword_counts:
                aggregated_keyword_counts[keyword] = count
            else:
                aggregated_keyword_counts[keyword] += count
    total_occurrences = sum(aggregated_keyword_counts.values())
    return {keyword: (count / total_occurrences) * 100 for keyword, count in aggregated_keyword_counts.items()}

# Example usage
if __name__ == '__main__':
    requested_data = ['Percentage', 'err_codes', 'Occurrences', 'Total']
    get_RTA_data(requested_data)  # Request and print multiple types of data at once
