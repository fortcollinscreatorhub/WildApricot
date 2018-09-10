#!/bin/bash

script_dir="$(dirname "$0")"
app_dir="$(cd "${script_dir}"/.. && pwd)"

export LC_ALL=C.UTF-8
export LANG=C.UTF-8

. "${app_dir}/venv/bin/activate"

if [[ "$1" == -* ]]; then
  #exec python3 "${app_dir}/bin/generate-acls.py" --noauth_local_webserver "$@"
  exec python3 "${app_dir}/bin/generate-acls-WA.py" "$@"
fi

if [ "${GEN_ACLS_MAIL_WRAP}" == "" ]; then
  acl_report="${app_dir}/var/log/acl-report.log"
  GEN_ACLS_MAIL_WRAP=1 "$0" "$@" > "${acl_report}" 2>&1
  ret=$?
  mail -s "HAL ACL update log" sysadmin@fortcollinscreatorhub.org < "${acl_report}"
  cat "${acl_report}"
  exit ${ret}
fi

acl_orig_dir="${app_dir}/var/acls-orig"
acl_new_dir="$1"
rm -rf "${acl_orig_dir}"
mkdir -p "${acl_new_dir}"
cp -r "${acl_new_dir}" "${acl_orig_dir}"

acl_dl_log="${app_dir}/var/log/acl-download.log"
#python3 "${app_dir}/bin/generate-acls.py" --noauth_local_webserver "${acl_new_dir}" > "${acl_dl_log}" 2>&1
python3 "${app_dir}/bin/generate-acls.py" "${acl_new_dir}" > "${acl_dl_log}" 2>&1
ret=$?
if [ ${ret} -ne 0 ]; then
  echo DOWNLOAD LOG:
  cat "${acl_dl_log}"
  exit ${ret}
fi

acl_diff_log="${app_dir}/var/log/acl-diff.log"
diff -urN "${acl_orig_dir}" "${acl_new_dir}" > "${acl_diff_log}" 2>&1
ret=$?
echo ACL DIFF:
cat "${acl_diff_log}"
echo
echo
echo
echo
echo DOWNLOAD LOG:
cat "${acl_dl_log}"
exit 0
