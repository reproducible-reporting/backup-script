# BTRFS + Borg backup script

You use this script at your own risk.
You are the only person responsible for any damage caused by the use of this script.

This script is distributed under the terms of the MIT license.
See the [LICENSE](LICENSE) file for more details.

## Basic guidelines

Ideally, backups are performed by a dedicated user account with access to user data and backups.
Users, whose data is being backed up, should only have read access to the backups.
This way, they (or any malware running in their account) cannot damage the backed up data.

To run the script on a regular basis,
you can write a simple shell script that calls the python script,
and then add this shell script to the crontab of the backup account.

For practical details on how to use the script,
see the docstring at the top of the file [`backup.py`](backup.py).
