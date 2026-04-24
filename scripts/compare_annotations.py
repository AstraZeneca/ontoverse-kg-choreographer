import ast
import pickle

# Load the annotation results from the pickle file
file_path = "path/to/your/cuiAnnotationsCombined.pk"  # Replace with your pickle file path
with open(
    file_path, "rb"
) as file:
    annotation_results = pickle.load(file)

annotation_results_converted = {}
for paper_id, cuis in annotation_results.items():
    if cuis:
        annotation_results_converted[int(paper_id)] = cuis

# Load and parse the given results from the text file
given_results_file = "pipeline_data/annotations.txt"  # Replace with your text file path

# Read the contents of the text file
with open(given_results_file, encoding="utf-8") as f:
    data = f.read()
# Parse the string representation of the dictionary into an actual dictionary
try:
    given_results = ast.literal_eval(data)
except Exception as e:
    print(f"Error parsing the given results file: {e}")
    exit(1)

# Remove papers with no CUIs from the given results
given_results_converted = {}
for paper_id, cuis in given_results.items():
    paper_id = int(paper_id)
    if cuis:
        given_results_converted[paper_id] = cuis

print(len(annotation_results))
print(len(given_results_converted))
assert len(annotation_results) == len(given_results_converted)

# Compare CUIs for each paper and check if they are exactly the same
all_papers_match = True  # Flag to check if all papers have matching CUIs

for paper_id in annotation_results_converted:

    # Get CUIs from the loaded annotation results
    # model_cuis = set(annotation_results.get(paper_id, []))
    model_cuis = set(annotation_results_converted[paper_id])
    print(f"Paper {paper_id}: {len(model_cuis)} CUIs")
    # Get CUIs from the given results
    given_cuis = set(given_results_converted[paper_id])

    if model_cuis == given_cuis:
        print(f"Paper {paper_id}: CUIs match exactly.")
        print(sorted(model_cuis))
    else:
        all_papers_match = False
        # Find differences
        missing_cuis = given_cuis - model_cuis
        extra_cuis = model_cuis - given_cuis

        print(f"Paper {paper_id}: CUIs do not match.")
        if missing_cuis:
            print(f"  CUIs missing in model results: {sorted(missing_cuis)}")
        if extra_cuis:
            print(f"  Extra CUIs in model results: {sorted(extra_cuis)}")
        print()

if all_papers_match:
    print("All papers have matching CUIs.")
else:
    print("Not all papers have matching CUIs.")
