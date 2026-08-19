# Copyright (c) 2023-2024, NVIDIA CORPORATION.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Implementation of <shimplify> Ant task in Python2.7 for Jython

Simplifies the prior version-range directory system

The if=false version is run by default during the generate-sources phase. If the user is
to run shimplify to perform file modifications for converting to the new system or to add a new
shim, it is recommended albeit not required to do it in a dedicated run after running `mvn install`.

```bash
mvn clean install -DskipTests
mvn generate-sources
```

Switches:

shimplify               - property passed to task attribute `if` whether to modify files
shimplify.add.base      - old buildver to base the new one provided by shimplify.add.shim
shimplify.add.shim      - add new shim/buildver based on the one provided by shimplify.add.base
shimplify.dirs          - comma-separated list of dirs to modify, supersedes shimplify.shims
shimplify.move          - property to allow moving files to canonical location, otherwise update
                          without moving
shimplify.overwrite     - property to allow shimplify executing file changes repeatedly,
                          error out otherwise
shimplify.shims         - comma-separated list of shims to simplify instead of all, superseded by
                          shimplify.dirs
shimplify.remove.shim   - drop support for shim/buildver, its exclusive files are removed
shimplify.trace         - property to enable trace logging

If the task attribute "if" evaluates to false shimplify does not alter files on the filesystem.
The task merely suggests to consolidate shim directories if it finds some shims that are comprised
of multiple directories not shared with other shims. The prior design expects to find only one such
directory per shim.

If the task attribute "if" evaluates to true shimplify is allowed to make changes on the filesystem.
It is expected that this is only done under a local git repo for an easy undo (otherwise making
manual backups is recommended). Undo typically involves three steps

First undo all potential renames

git restore --staged sql-plugin tests

Then undo all non-staged changes including produced by the command above.

git restore sql-plugin tests

Optionally review and remove empty directories

git clean -f -d [--dry-run]

Each shim Scala/Java file receives a comment describing all Spark builds it
belongs to. Lines are sorted by the Spark `buildver` lexicographically.
Each line is assumed to be a JSON to keep it extensible.

/*** spark-rapids-shim-json-lines
{"spark": "330"}
{"spark": "331"}
spark-rapids-shim-json-lines ***/

The canonical location of a source file shared by multiple shims is
src/main/<top_buildver_in_the_comment>

You can find all shim files for a particular shim, e.g. 330, easily by executing:
git grep '{"spark": "330"}' '*.java' '*.scala'
"""

import errno
import json
import logging
import os
import re
import subprocess
from functools import partial


def __project():
    """
    Wraps access to the implicitly provided Ant project reference[1] to reduce undefined-name
    linting warnings

    TODO Pass only Python types if possible

    [1] https://ant.apache.org/manual/api/org/apache/tools/ant/Project.html
    """
    return project


def __task():
    """
    Wraps access to the implicitly provided Ant task attributes map to reduce
    undefined-name linting warnings
    """
    return self


def __fail(message):
    "Fails this task with the error message"
    __task().fail(message)


def __attributes():
    """
    Wraps access to the implicitly provided Ant task attributes map to reduce
    undefined-name linting warnings

    TODO Pass only Python types if possible
    """
    return attributes


def __ant_proj_prop(name):
    """Returns an Ant project property value as a Python string"""
    prop_val = __project().getProperty(name)
    return None if prop_val is None else str(prop_val)


def __ant_attr(name):
    """Returns this Ant task attribute value as a Python string"""
    attr_val = __attributes().get(name)
    return None if attr_val is None else str(attr_val)


def __is_enabled_property(prop_name):
    """Returns True if the required project property is set to true"""
    assert prop_name is not None, "Invalid property: None"
    prop_val = __ant_proj_prop(prop_name)
    return str(True).lower() == prop_val


def __is_enabled_attr(attr):
    """Returns True if the required project property is set to true"""
    assert attr is not None, "Invalid attribute: None"
    attr_val = __ant_attr(attr)
    return attr_val is not None and __is_enabled_property(attr_val)


def __csv_ant_prop_as_arr(name):
    """Splits a CSV value for a property into a list"""
    prop_val = __ant_proj_prop(name)
    return __csv_as_arr(prop_val)


def __csv_as_arr(str_val):
    """Splits a string CSV value into a list, returns [] if undefined or empty"""
    if str_val in (None, ''):
        return []
    else:
        return str_val.translate(None, ' ' + os.linesep).split(',')

__src_basedir = __ant_proj_prop('shimplify.src.basedir') or str(__project().getBaseDir())

__should_add_comment = __is_enabled_attr('if')

# should we move files?
__should_move_files = __is_enabled_property('shimplify.move')

# enable log tracing?
__should_trace = __is_enabled_property('shimplify.trace')

__add_shim_buildver = __ant_proj_prop('shimplify.add.shim')
__add_shim_base = __ant_proj_prop('shimplify.add.base')

__remove_shim_buildver = __ant_proj_prop('shimplify.remove.shim')

# allowed to overwrite the existing comment?
__should_overwrite = (__is_enabled_property('shimplify.overwrite')
                      or __add_shim_buildver is not None
                      or __remove_shim_buildver is not None)


__shim_comment_tag = 'spark-rapids-shim-json-lines'
__opening_shim_tag = '/*** ' + __shim_comment_tag
__closing_shim_tag = __shim_comment_tag + ' ***/'
__shims_arr = sorted(__csv_ant_prop_as_arr('shimplify.shims'))
__dirs_to_derive_shims = sorted(__csv_ant_prop_as_arr('shimplify.dirs'))

__log = logging.getLogger('shimplify')
__log.setLevel(logging.DEBUG if __should_trace else logging.INFO)
__ch = logging.StreamHandler()
__ch.setFormatter(logging.Formatter('%(name)s - %(levelname)s - %(message)s'))
__log.addHandler(__ch)
__all_shims_arr = sorted(__ant_proj_prop("included_buildvers").split(", "))
__shims_arr = __all_shims_arr if not __shims_arr else __shims_arr

__shim_dir_pattern = re.compile(r'spark\d{3}')
__shim_comment_pattern = re.compile(re.escape(__opening_shim_tag) +
                                    r'\n(.*)\n' +
                                    re.escape(__closing_shim_tag), re.DOTALL)
__spark_version_classifier = '$_spark.version.classifier_'
__spark_version_placeholder = re.escape(__spark_version_classifier)
__package_pattern = re.compile('package .*' + '(' + __spark_version_placeholder + ')')
def __upsert_shim_json(filename, bv_list):
    with open(filename, 'r') as file:
        contents = file.readlines()
    __delete_prior_comment_if_allowed(contents, __shim_comment_tag, filename)
    shim_comment = [__opening_shim_tag]
    for build_ver in bv_list:
        shim_comment.append(json.dumps({'spark':  build_ver}))
    shim_comment.append(__closing_shim_tag)
    shim_comment = map(lambda x: x + '\n', shim_comment)
    __log.debug("Inserting comment %s to %s", shim_comment, filename)
    package_line = next(i for i in range(len(contents)) if str(contents[i]).startswith('package'))
    __log.debug("filename %s package_line_number=%d", filename, package_line)
    for i in range(len(shim_comment)):
        contents.insert(package_line + i, shim_comment[i])
    with open(filename, 'w') as file:
        file.writelines(contents)


def __delete_prior_comment_if_allowed(contents, tag, filename):
    opening_shim_comment_line = None
    closing_shim_comment_line = None
    try:
        opening_shim_comment_line = next(i for i in range(len(contents))
                                         if tag in str(contents[i]))
        shim_comment_and_below = range(opening_shim_comment_line + 1, len(contents))
        closing_shim_comment_line = next(i for i in shim_comment_and_below
                                         if tag in str(contents[i]))
    except StopIteration as si_exc:
        if (opening_shim_comment_line is not None) and (closing_shim_comment_line is None):
            __fail("%s: no closing comment for %s:%d"
                   % (si_exc, filename, opening_shim_comment_line))
    if opening_shim_comment_line is None:
        # no work
        return
    if not __should_overwrite:
        __fail("found shim comment from prior execution at %s:%d, use -Dshimplify.overwrite=true "
               "to overwrite" % (filename, opening_shim_comment_line))
    assert (opening_shim_comment_line is not None) and (closing_shim_comment_line is not None)
    __log.debug("removing comments %s:%d:%d", filename, opening_shim_comment_line,
                closing_shim_comment_line)
    del contents[opening_shim_comment_line:(closing_shim_comment_line + 1)]


def __git_rename_or_copy(shim_file, owner_shim, from_shim=None):
    __log.debug("git rename %s to the canonical dir of %s", shim_file, owner_shim)
    parent_pom_dir = __ant_proj_prop('spark.rapids.source.basedir')
    # sql-plugin/src/main/330/scala/org/apache/spark/...
    rel_path = os.path.relpath(shim_file, parent_pom_dir)
    __log.debug("spark-rapids root dir: %s", parent_pom_dir)
    __log.debug("shim file path relative to root dir: %s", rel_path)
    path_parts = rel_path.split(os.sep)
    owner_path_comp = "spark%s" % owner_shim

    from_path_comp = None
    if from_shim is not None:
        # may have to update package path
        from_path_comp = "spark%s" % from_shim
        path_parts = [p.replace(from_path_comp, owner_path_comp) for p in path_parts]

    # to enable both builds at the same time the original location should change too
    # <module>/src/test/331 =>  <module>/src/test/spark331
    path_parts[3] = owner_path_comp
    new_shim_file = os.sep.join([parent_pom_dir] + path_parts)
    if shim_file == new_shim_file:
        __log.info("%s is already at the right location, skipping git rename", shim_file)
    else:
        new_shim_dir = os.path.dirname(new_shim_file)
        __log.debug("creating new shim path %s", new_shim_dir)
        __makedirs(new_shim_dir)
        if from_path_comp is None:
            shell_cmd = ['git', 'mv', shim_file, new_shim_file]
            __shell_exec(shell_cmd)
        else:
            with open(shim_file, 'r') as src_shim_fh:
                with open(new_shim_file, 'w') as dst_shim_fh:
                    content = src_shim_fh.read()
                    dst_content = content.replace(from_path_comp, owner_path_comp)
                    dst_shim_fh.write(dst_content)
            git_add_cmd = ['git', 'add', new_shim_file]
            __shell_exec(git_add_cmd)
    return new_shim_file


def __shell_exec(shell_cmd):
    ret_code = subprocess.call(shell_cmd)
    if ret_code != 0:
        __fail("failed to execute %s" % shell_cmd)


def __makedirs(new_dir):
    try:
        __log.debug("__makedirs %s", new_dir)
        os.makedirs(new_dir)
    except OSError as ose:
        if not (ose.errno == errno.EEXIST and os.path.isdir(new_dir)):
            raise


def task_impl():
    """Ant task entry point """
    __log.info('# Starting Jython Task Shimplify #')
    config_format = """#   config:
#           shimplify.src.baseDir=%s
#           shimplify (if)=%s
#           shimplify.add.base=%s
#           shimplify.add.shim=%s
#           shimplify.dirs=%s
#           shimplify.move=%s
#           shimplify.overwrite=%s
#           shimplify.shims=%s
#           shimplify.trace=%s"""
    __log.info(config_format,
               __src_basedir,
               __should_add_comment,
               __add_shim_base,
               __add_shim_buildver,
               __dirs_to_derive_shims,
               __should_move_files,
               __should_overwrite,
               __shims_arr,
               __should_trace)
    __log.info("review changes and `git restore` if necessary")
    buildvers_from_dirs = []
    dirs2bv = {}

    for prop_pattern in ["spark%s.sources", "spark%s.test.sources"]:
        per_pattern_dir_map = __build_dirs_to_buildvers_map(prop_pattern)
        __log.debug("Map dirs2bv = %s", per_pattern_dir_map)
        __warn_shims_with_multiple_dedicated_dirs(per_pattern_dir_map)
        dirs2bv.update(per_pattern_dir_map)

    # restrict set of dirs to shimplify.dirs?
    for dir, buildvers in dirs2bv.items():
        for dir_substr in __dirs_to_derive_shims:
            if dir_substr in dir:
                buildvers_from_dirs += buildvers

    buildvers_from_dirs_sorted_deduped = sorted(set(buildvers_from_dirs))
    if len(buildvers_from_dirs_sorted_deduped) > 0:
        __log.info("shimplify.dirs = %s, overriding shims from dirs: %s", __dirs_to_derive_shims,
                   buildvers_from_dirs_sorted_deduped)
        __shims_arr[:] = buildvers_from_dirs_sorted_deduped

    if __should_add_comment:
        __log.info('Shim layout is being updated! Review and git commit (or restore to undo)'
                   '-Dshimplify=true`. New symlinks will be generated in a regular build with the '
                   'default -Dshimplify=false')
        __shimplify_layout()
    else:
        __log.info('Shim layout is not updated! If desired invoke '
                   '`mvn generate-sources -Dshimplify=true` to manipulate shims')
        __generate_symlinks()


def __generate_symlinks():
    """
    link
    <module>/src/<main|test>/spark<buildver>/scala/<package_path>/SomeClass.scala
    <module>/target/<buildver>/generated/src/<main|test>/scala/<package_path>/SomeClass.scala
    """
    buildver = __ant_proj_prop('buildver')
    for src_type in ['main', 'test']:
        __log.info("# generating symlinks for shim %s %s files", buildver, src_type)
        __traverse_source_tree_of_all_shims(
            src_type,
            partial(__generate_symlink_to_file, buildver=buildver, src_type=src_type))

def __traverse_source_tree_of_all_shims(src_type, func):
    """Walks src/<src_type>/sparkXYZ"""
    base_dir = __src_basedir
    src_root = os.path.join(base_dir, 'src', src_type)
    for dir, subdirs, files in os.walk(src_root, topdown=True):
        if dir == src_root:
            subdirs[:] = [d for d in subdirs if re.match(__shim_dir_pattern, d)]
        for f in files:
            shim_file_path = os.path.join(dir, f)
            __log.debug("processing shim comment at %s", shim_file_path)
            with open(shim_file_path, 'r') as shim_file:
                shim_file_txt = shim_file.read()
                shim_match = __shim_comment_pattern.search(shim_file_txt)
                assert shim_match is not None and shim_match.groups(), \
                    "no shim comment located in %s, " \
                    "orphan shim files should be deleted" % shim_file_path
                shim_arr = shim_match.group(1).split(os.linesep)
                assert len(shim_arr) > 0, "invalid empty shim comment,"\
                    "orphan shim files should be deleted"
                build_ver_arr = map(lambda x: str(json.loads(x).get('spark')), shim_arr)
                __log.debug("extracted shims %s", build_ver_arr)
                assert build_ver_arr == sorted(build_ver_arr),\
                    "%s shim list is not properly sorted: %s" % (shim_file_path, build_ver_arr)
                func(shim_file_path=shim_file_path, build_ver_arr=build_ver_arr, shim_file_txt=shim_file_txt)

def __generate_symlink_to_file(buildver, src_type, shim_file_path, build_ver_arr, shim_file_txt):
    if buildver in build_ver_arr:
        project_base_dir = str(__project().getBaseDir())
        base_dir = __src_basedir
        src_root = os.path.join(base_dir, 'src', src_type)
        target_root = os.path.join(project_base_dir, 'target', "spark%s" % buildver, 'generated', 'src',
                                   src_type)
        first_build_ver = build_ver_arr[0]
        __log.debug("top shim comment %s", first_build_ver)
        shim_file_rel_path = os.path.relpath(shim_file_path, src_root)
        expected_prefix = "spark%s%s" % (first_build_ver, os.sep)
        assert shim_file_rel_path.startswith(expected_prefix), "Unexpected: %s is not prefixed " \
               "by %s" % (shim_file_rel_path, expected_prefix)
        shim_file_rel_path_parts = shim_file_rel_path.split(os.sep)
        # drop spark3XY from spark3XY/scala/com/nvidia
        target_file_parts = shim_file_rel_path_parts[1:]
        target_rel_path = os.sep.join(target_file_parts)
        target_shim_file_path = os.path.join(target_root, target_rel_path)
        __log.debug("creating symlink %s -> %s", target_shim_file_path, shim_file_path)
        __makedirs(os.path.dirname(target_shim_file_path))
        package_match = __package_pattern.search(shim_file_txt)
        if __should_overwrite or package_match:
            __remove_file(target_shim_file_path)
        if package_match:
            with open(target_shim_file_path, mode='w') as f:
                f.write(shim_file_txt[0:package_match.start(1)])
                f.write("spark")
                f.write(buildver)
                f.write('\n')
                f.write('''
/*
!!! DO NOT EDIT THIS FILE !!!

This file has been generated from the original

%s

by interpolating $_spark.version.classifier_=%s

Be sure to edit the original file if required

*/
                ''' % (shim_file_path, 'spark' + buildver))
                f.write(shim_file_txt[package_match.end(1):])
        else:
            __symlink(shim_file_path, target_shim_file_path)


def __symlink(src, target):
    try:
        os.symlink(src, target)
    except OSError as ose:
        if ose.errno != errno.EEXIST:
            raise


def __remove_file(target_shim_file_path):
    try:
        os.remove(target_shim_file_path)
    except OSError as ose:
        # ignore non-exisiting files
        if ose.errno != errno.ENOENT:
            raise


def __shimplify_layout():
    __log.info("executing __shimplify_layout")
    assert ((__add_shim_buildver is None) and (__add_shim_base is None) or
            (__add_shim_buildver is not None) and (__add_shim_base is not None)),\
           "shimplify.add.shim cannot be specified without shimplify.add.base and vice versa"
    assert __add_shim_base is None or __add_shim_base in __shims_arr,\
           "shimplify.add.base is not in %s" % __shims_arr
    assert __add_shim_buildver is None or __remove_shim_buildver is None,\
           "Adding and deleting a shim in a single invocation is not supported!"
    # map file -> [shims it's part of]
    files2bv = {}

    # if the user allows to overwrite / reorganize shimplified shims,
    # commonly while adding or removing shims we must include new shim locations
    if __should_overwrite:
        for src_type in ['main', 'test']:
            __traverse_source_tree_of_all_shims(
                src_type,
                partial(__update_files2bv, files2bv=files2bv))

    # adding a new shim?
    if __add_shim_buildver is not None:
        __add_new_shim_to_file_map(files2bv)

    if __remove_shim_buildver is not None:
        __remove_shim_from_file_map(files2bv)

    for shim_file, bv_list in files2bv.items():
        if len(bv_list) == 0:
            if __should_move_files:
                __log.info("Removing orphaned file %s", shim_file)
                __shell_exec(['git', 'rm', shim_file])
            else:
                __log.info("Detected an orphaned shim file %s, possibly while removing a shim."
                           " git rm it manually or rerun with -Dshimplify.move=true",
                           shim_file)
        else:
            sorted_build_vers = sorted(bv_list)
            __log.debug("calling upsert_shim_json on shim_file %s bv_list=%s", shim_file,
                        sorted_build_vers)
            owner_shim = sorted_build_vers[0]
            if owner_shim in __shims_arr:
                __upsert_shim_json(shim_file, sorted_build_vers)
                if __should_move_files:
                    __git_rename_or_copy(shim_file, owner_shim)


def __update_files2bv(files2bv,
                      # TODO an anachronism requirement: that the following two params
                      # have the same name along generate_symlink_file
                      shim_file_path,
                      build_ver_arr,
                      #
                      **kwargs):
    assert shim_file_path not in files2bv.keys(), "new path %s %s should be "\
        "encountered only once, current map\n%s" % (shim_file_path, build_ver_arr, files2bv)
    __log.debug("Adding %s %s to files to shim map", shim_file_path, build_ver_arr)
    files2bv[shim_file_path] = build_ver_arr


def __add_new_shim_to_file_map(files2bv):
    if __add_shim_buildver not in __all_shims_arr:
        __log.warning("all.buildvers doesn't contain %s please look at build/get_buildvers.py", __add_shim_buildver)
    if __add_shim_buildver not in __shims_arr:
        # TODO  should we just bail and ask the user to add to all.buildvers manually first?
        __shims_arr.append(__add_shim_buildver)

    # copy keys to be able to modify the original dictionary while iterating
    for shim_file in set(files2bv.keys()):
        bv_list = files2bv[shim_file]
        if __add_shim_base in bv_list:
            # adding a lookalike
            # case 1) dedicated per-shim files with a spark${buildver} in the package path,
            #         which implies a substring like /spark332/ occurs at least twice in the path:
            #         CLONE the file with modifications
            # case 2) otherwise simply add the new buildver to the files2bv[shimfile] mapping
            #
            if shim_file.count("%sspark%s%s" % (os.sep, __add_shim_base, os.sep)) > 1:
                assert len(bv_list) == 1, "Per-shim file %s is expected to belong to a single "\
                        "shim, actual shims: %s" % (shim_file, bv_list)
                new_shim_file = __git_rename_or_copy(shim_file, __add_shim_buildver,
                                                     from_shim=__add_shim_base)
                # schedule new file for comment update
                __log.info("Adding a per-shim file %s for %s", new_shim_file,
                           __add_shim_buildver)
                files2bv[new_shim_file] = [__add_shim_buildver]
            else:
                # TODO figure out why __add_shim_buildver is unicode class, not a simple str
                # and if we care
                __log.info("Appending %s to %s for %s", __add_shim_buildver, bv_list, shim_file)
                bv_list.append(__add_shim_buildver)


def __remove_shim_from_file_map(files2bv):
    __log.info("Removing %s shim, pom.xml should be updated manually.", __remove_shim_buildver)
    # copy keys to be able to modify the original dictionary while iterating
    for shim_file in set(files2bv.keys()):
        bv_list = files2bv[shim_file]
        try:
            bv_list.remove(__remove_shim_buildver)
        except ValueError as ve:
            # __remove_shim_buildver is not in the list
            __log.debug("%s: file %s does not belong to shim %s, skipping it", ve, shim_file,
                        __remove_shim_buildver)
            pass


def __warn_shims_with_multiple_dedicated_dirs(dirs2bv):
    # each shim has at least one dedicated dir, report shims
    # with multiple dedicated dirs because they can be merged
    single_shim_dirs = {dir: shims[0] for dir, shims in dirs2bv.items() if len(shims) == 1}
    __log.debug("shims with exclusive dirs %s", single_shim_dirs)
    multi_dir_shims = {}
    for dir, single_shim in single_shim_dirs.items():
        if single_shim in multi_dir_shims.keys():
            multi_dir_shims[single_shim].append(dir)
        else:
            multi_dir_shims[single_shim] = [dir]
    for shim, dirs in multi_dir_shims.items():
        if len(dirs) > 1:
            __log.warning("Consider consolidating %s, it spans multiple dedicated directories %s",
                          shim, dirs)


def __build_dirs_to_buildvers_map(prop_pattern):
    dirs2bv = {}
    for build_ver in __all_shims_arr:
        __log.debug("updating dirs2bv for %s", build_ver)
        shim_dirs = __csv_ant_prop_as_arr(prop_pattern % build_ver)
        for dir in shim_dirs:
            if dir not in dirs2bv.keys():
                dirs2bv[dir] = [build_ver]
            else:
                dirs2bv[dir] += [build_ver]
    __log.debug("Map build_ver -> shim_dirs %s" % dirs2bv)
    return dirs2bv


task_impl()
