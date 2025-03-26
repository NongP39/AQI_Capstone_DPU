import json

with open("/workspaces/DPU_Capstone/dags/data.json", "r") as f:
    data = json.load(f)

assert data.get("status") != "fail"
#print(data.get("status") != "success")