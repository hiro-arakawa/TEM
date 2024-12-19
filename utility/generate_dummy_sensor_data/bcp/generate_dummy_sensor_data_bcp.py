import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta


def generate_tags(factory, num_tags=3000, tag_file="tags.csv"):
    """
    Generate unique tags for a factory and save them to a file.
    Tags are in the format: 2-digit + 3 + 3-digit hexadecimal (6 characters total).
    
    Parameters:
        factory (str): Factory code.
        num_tags (int): Number of tags to generate for the factory.
        tag_file (str): Path to save the generated tags.
    
    Returns:
        pd.DataFrame: DataFrame containing tags for the factory.
    """
    # Check if tags already exist
    if os.path.exists(tag_file):
        tag_data = pd.read_csv(tag_file)
        if factory in tag_data["factory"].values:
            return tag_data[tag_data["factory"] == factory]
    
    # Generate tags
    tags = [f"{i:02}3{j:03X}" for i in range(100) for j in range(256)][:num_tags]
    tag_df = pd.DataFrame({"factory": factory, "tag": tags})
    
    # Save tags to file
    if os.path.exists(tag_file):
        existing_tags = pd.read_csv(tag_file)
        tag_df = pd.concat([existing_tags, tag_df], ignore_index=True)
    tag_df.to_csv(tag_file, index=False)
    
    return tag_df[tag_df["factory"] == factory]

def generate_bcp_compatible_data(date, factory, tags):
    """
    Generate dummy data for all tags in a factory.
    
    Parameters:
        date (str): Target date in "YYYY-MM-DD" format.
        factory (str): Factory code.
        tags (list): List of tags for the factory.
    
    Returns:
        pd.DataFrame: DataFrame containing BCP-compatible dummy data.
    """
    base_date = datetime.strptime(date, "%Y-%m-%d")
    data_rows = []
    
    for tag in tags:
        # Generate initial random values for data1, data2, data3
        initial_values = {
            "data1": np.random.uniform(100, 1000),
            "data2": np.random.uniform(100, 1000),
            "data3": np.random.uniform(100, 1000)
        }
        
        # Generate values with ~2% variation per hour
        data = {"data1": [], "data2": [], "data3": []}
        for key, initial_value in initial_values.items():
            current_value = initial_value
            for _ in range(30):
                variation = np.random.uniform(-0.02, 0.02)  # ±2% variation
                current_value *= (1 + variation)
                data[key].append(round(current_value, 2))
        
        # Generate data assurance category (d0)
        d0_values = [1 if np.random.uniform(0, 1) > 0.1 else 2 for _ in range(30)]  # 90% normal (1), 10% abnormal (2)
        
        # Create a single row for 30 hours of data
        row = {
            "factory": factory,
            "tag": tag,
            "local_tag": tag,  # Same as tag
            "local_id": tag,   # Same as tag
            "date": base_date.date(),
            "name": f"Sensor {tag}",
            "unit": "unit",
            "data_division": 1,  # Example division
            "last_update": datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Ensure this is added
        }
        # Add d0_0 to d3_29
        for i in range(30):  # 30 hours
            row[f"d0_{i}"] = d0_values[i]  # Data assurance category
            row[f"d1_{i}"] = data["data1"][i]
            row[f"d2_{i}"] = data["data2"][i]
            row[f"d3_{i}"] = data["data3"][i]
        
        data_rows.append(row)
    
    return pd.DataFrame(data_rows)


def generate_all_factories(date, factory_info, tag_file="utility/generate_dummy_sensor_data/bcp/tags.csv"):
    """
    Generate data for all factories on the specified date.
    
    Parameters:
        date (str): Target date in "YYYY-MM-DD" format.
        factory_info (dict): Dictionary with factory codes as keys and tag counts as values.
        tag_file (str): Path to save the generated tags.
    
    Returns:
        pd.DataFrame: DataFrame containing data for all factories.
    """
    all_data = []
    for factory, tag_count in factory_info.items():
        # Generate or load tags
        tags = generate_tags(factory, num_tags=tag_count, tag_file=tag_file)["tag"].tolist()
        # Generate data for the factory
        factory_data = generate_bcp_compatible_data(date, factory, tags)
        all_data.append(factory_data)
    # Ensure all columns, including 'last_update', are preserved
    return pd.concat(all_data, ignore_index=True)

if __name__ == "__main__":
    # Factory information
    factory_info = {
        "A": 3000, "F2": 3000, "G": 3000, "H": 3000, "J": 3000, "K": 3000,
        "L": 3000, "M": 3000, "N": 3000, "P": 3000, "Q": 3000, "R": 3000,
        "S": 3000, "T": 3000, "Y": 3000
    }
    # Target date
    target_date = "2024-12-20"

    # Generate data
    all_data = generate_all_factories(target_date, factory_info)

    # Define column order to match the target table
    column_order = [
        "factory", "tag", "date", "local_tag", "local_id", "name", "unit", "data_division"
    ]
    # Add dynamically generated columns d0_0 to d3_29
    for i in range(30):
        column_order.extend([f"d0_{i}", f"d1_{i}", f"d2_{i}", f"d3_{i}"])
    column_order.append("last_update")  # Ensure 'last_update' is the last column

    # Save to CSV with specified column order
    output_file = "ututility/generate_dummy_sensor_data/bcp/bcp_all_factories_data.csv"
    all_data[column_order].to_csv(output_file, index=False, na_rep="NULL")
    print(f"Data for all factories saved to {output_file}")
