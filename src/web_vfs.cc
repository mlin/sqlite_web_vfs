/*
** SQLite loadable extension providing web VFS
*/
#include <sqlite3ext.h>
extern "C" {
SQLITE_EXTENSION_INIT1
}
#include "web_vfs.h"

/*************************************************************************************************/

/*
** This routine is called when the extension is loaded.
** Register the new VFS.
*/
extern "C" int sqlite3_webvfs_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
    SQLITE_EXTENSION_INIT2(pApi);
    spdlog::set_level(spdlog::level::debug);
    int rc = SQLITE_OK;
    rc = (new WebVFS())->Register("web");
    if (rc == SQLITE_OK)
        rc = SQLITE_OK_LOAD_PERMANENTLY;
    return rc;
}
