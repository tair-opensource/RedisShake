import json
import os

commands_dir = "./commands"
files = os.listdir(commands_dir)
table = {}
container = set()
files = sorted(files)
for file in files:
    content = json.load(open(f"{commands_dir}/{file}"))
    for cmd_name, j in content.items():
        print(cmd_name)
        if cmd_name in ("SORT", "SORT_RO", "MIGRATE"):
            continue

        group = j["group"]
        key_specs = []
        if "key_specs" in j:
            for key_spec in j["key_specs"]:
                begin_search = key_spec["begin_search"]
                find_keys = key_spec["find_keys"]
                key_specs.append({
                    "begin_search": begin_search,
                    "find_keys": find_keys
                })
        if "container" in j:
            cmd_name = j["container"] + "-" + cmd_name
            container.add(j["container"])

        if group not in table:
            table[group] = {}
        table[group][cmd_name] = key_specs

container = sorted(container)
with open("table.json", "w") as f:
    json.dump({
        "table": table,
        "container": list(container)
    }, f, indent=4)
