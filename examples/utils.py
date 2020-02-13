import os
import kobo.rpmlib


def get_sigkey(rpmfile):
    """
    If the RPM is signed, return the RPM's sigkey.
    rpmfile: path to an RPM
    """
    rpmhead = kobo.rpmlib.get_rpm_header(rpmfile)
    rpmsig = kobo.rpmlib.get_keys_from_header(rpmhead)

    if rpmsig is None:
        raise RuntimeError("Invalid RPM signature: %s is unsigned" % rpmfile)
    else:
        return str.lower(rpmsig)


def cdn_path(file_name, file_path, checksum):
    """
    Get the origin cdn_path for the given file_info.
    file_name: full name
    file_path: full path to file, unless file is in cwd
    checksum: the file's checksum
    """
    if file_name.endswith(".rpm"):
        file_info = kobo.rpmlib.parse_nvra(file_name)
        # the given information for creating the cdn_path
        # so as an example for the rpm
        # ipa-admintools-4.4.0-14.el7_3.1.1.noarch.rpm
        cdn_path = os.path.join(
            "/origin/rpms",
            # ipa-admintools part
            file_info["name"],
            # 4.4.0 version part
            file_info["version"],
            # 14.el7_3.1.1
            file_info["release"],
            # fd431d51 - used by signing tools
            get_sigkey(file_path),
            # ipa-admintools-4.4.0-14.el7_3.1.1.noarch.rpm
            # which seems to be the actual rpm that sits within
            # the directory.
            file_name,
        )
    else:
        cdn_path = os.path.join(
            "/origin/files", "sha256", checksum[:2], checksum, file_name
        )
    return cdn_path
