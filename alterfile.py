from pathlib import Path

for p in Path('.').glob('*.sql'):
    with open(f"{p.name}") as reader, open(f"{p.name}", 'r+') as writer:
        content = reader.read();
        writer.seek(0)
        writer.write("-- liquibase formatted sql \n\n")
        writer.write("-- changeset ivycpg:24022024-2 labels:"+f"{p.name.split('.')[0]} \n")
        writer.write(content)
         
         
# for p in Path('.').glob('*.sql'):
    # with open(f"{p.name}") as reader, open(f"{p.name}", 'r+') as writer:
      # for line in reader:
        # if line.strip():
          # writer.write(line)
      # writer.truncate()
      