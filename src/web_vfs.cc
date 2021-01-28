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
    int rc = SQLITE_OK;
    rc = (new WebVFS::VFS())->Register("web");
    return rc != SQLITE_OK ? rc : SQLITE_OK_LOAD_PERMANENTLY;
}
