def execute(example_csv, config, p=None):
    if p:
        print(f"You passed p={p}, {config}")
    an_output_value = example_csv["A"] * 2
    return {"double_A": an_output_value}
