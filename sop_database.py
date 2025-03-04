from utils import embed_modules as em
from ctk_gui.ctk_windows import PopupGetText


def get_results(query='How do I optimize shipments in EU/UK markeptlaces?'):
    response = em.vector_search(query)
    for result in response.matches:
        problem = result.metadata['problem']
        solution = result.metadata['solution']
        relevance = result['score']
        dates = f"created on {result.metadata['date_created']}, modified on {result.metadata['date_modified']}"
        record_id = result.id

        print(f'''Problem: {problem}\nSolution: {solution}\nRelevance:{relevance}\nDates: {dates}\nID: {record_id}\n\n''')

def new_sop():
    problem = PopupGetText('Which problem does it address?', width=1000, height=50).return_value()
    solution = PopupGetText('What is the solution?', width=1000, height=100).return_value()
    result = em.add_record(problem, solution)
    print(result)

new_sop()
# get_results()
# print(em.delete_record_from_vector(key = '840fffa1-1d23-4b17-9c0e-ae54ee2ba211'))