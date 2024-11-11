import pandas as pd


def write_instance() -> None:

    with open("result/instance.txt", "w") as final:
        final.write("* Haltestellen\n*\n")

        with open("result/stoppoints.txt", "r") as stoppoints_file:
            stoppoints_content = stoppoints_file.readlines()

        stoppoints_content[0] = "$STOPPOINT:ID;Code;Name;Lat;Lon;VehCapacityForCharging"

        final.writelines(stoppoints_content)

        final.write("*\n* Linien\n*\n")

        with open("result/line.txt", "r") as lines_file:
            lines_content = lines_file.readlines()
        lines_content[0] = "$LINE:ID;Code;Name"

        final.writelines(lines_content)

        final.write("*\n* Linienfahrten\n*\n")

        with open("result/servicejourney.txt", "r") as servicejourney_file:
            servicejourney_content = servicejourney_file.readlines()
        servicejourney_content[0] = (
            "$SERVICEJOURNEY:ID;ID;LineID;FromStopID;DepTime;ToStopID;ArrTime;VehTypeGroupID;Distance;MinLayoverTime;MinAheadTime;MaxShiftBackwardSeconds;MaxShiftForwardSeconds"
        )
        final.writelines(servicejourney_content)

        final.write("*\n* Verbindungen\n*\n")

        with open("result/deadruntime.txt", "r") as deadruntime_file:
            deadruntime_file_content = deadruntime_file.readlines()
        deadruntime_file_content[0] = (
            "$DEADRUNTIME:FromStopID;ToStopID;FromTime;ToTime;Distance;RunTime"
        )
        final.writelines(deadruntime_file_content)

        final.write("*\n* Connections\n*\n")

        with open("result/connections.txt", "r") as connections_file:
            connections_file_content = connections_file.readlines()
        connections_file_content[0] = (
            "$CONNECTIONS:FromStopID;ToStopID;FromLineID;ToLineID;MinTransferTime"
        )
        final.writelines(connections_file_content)


if __name__ == "__main__":
    write_instance()
