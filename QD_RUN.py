from Quality_Detection.Quality_Detection import run_employee_dq_pipeline

if __name__ == "__main__":
    report = run_employee_dq_pipeline("Messy_Employee_dataset_v2.csv")
    print("\n Data Quality Report")

    for section, result in report.items():
        print(f"{section.upper()}")
        print(result)
        print("-" * 40)