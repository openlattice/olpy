POSTGRES_TO_OL = {
    "Binary": ["bytea"],
     "Boolean": ["bool"],
    "Byte": ["bytea"],
    "SByte": ["bytea"],
    "Date": ["date"],
    "Duration": ["interval"],
    "DateTimeOffset": ["timestamp with time zone"],
    "TimeOfDay": ["time with time zone"],
    "Decimal": ["numeric"],
    "Single": ["real"],
    "Double": ["double precision"],
    "Guid": ["uuid"],
    "Int16": ["bigint"],
    "Int32": ["bigint"],
    "Int64": ["bigint"],
    "String": ["text", "character varying"],
    "Geography": ["text", "character varying"],
    "GeographyPoint": ["text", "character varying"],
    'GeographyLineString': ["text", "character varying"],
    "GeographyPolygon": ["text", "character varying"],
    "GeographyMultiPoint": ["text", "character varying"],
    "GeographyMultiLineString": ["text", "character varying"],
    "GeographyMultiPolygon": ["text", "character varying"],
    "GeographyCollection": ["text", "character varying"],
    "Geometry": ["text", "character varying"],
    "GeometryPoint": ["text", "character varying"],
    "GeometryLineString": ["text", "character varying"],
    "GeometryPolygon": ["text", "character varying"],
    "GeometryMultiPoint": ["text", "character varying"],
    "GeometryMultiLineString": ["text", "character varying"],
    "GeometryMultiPolygon": ["text", "character varying"],
    "GeometryCollection": ["text", "character varying"]
}