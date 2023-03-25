import subprocess
import sys

# Command to run three times
command = "filebench -f "

filebench_workloads = ["filemicro_create.f", "filemicro_createfiles.f", "filemicro_createrand.f", "filemicro_delete.f", "filemicro_rread.f", "filemicro_rwritedsync.f", "filemicro_seqread.f", "filemicro_seqwrite.f", "filemicro_statfile.f", "filemicro_writefsync.f", "fileserver.f", "mongo.f", "varmail.f", "webserver.f"]
# Open file for appending

# filebench_workloads = ["filemicro_create.f"]


for file in filebench_workloads:
    with open(f"results/{file}", "a") as f:
        sys.stdout = f
        # Run command three times
        for i in range(3):
            # Execute the command
            run_command = command + file
            # run_command = "ls -la"
            result = subprocess.run(run_command.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
            f.write(result.stdout)
            # Write the output to the file

