# ----------------------------------------------------------------------------------------
class ClassTypes:
    AIOSQLITE = "dls_normsql.aiosqlite"
    AIOMYSQL = "dls_normsql.aiomysql"


# ----------------------------------------------------------------------------------------
class Tablenames:
    REVISION = "revision"


# ----------------------------------------------------------------------------------------
class CommonFieldnames:
    AUTOID = "autoid"
    UUID = "uuid"
    CREATED_ON = "created_on"


# ----------------------------------------------------------------------------------------
class RevisionFieldnames:
    CREATED_ON = CommonFieldnames.CREATED_ON
    NUMBER = "number"
