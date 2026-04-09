local schema_name = assert(
    os.getenv('LIVABILITY_IMPORT_SCHEMA'),
    'LIVABILITY_IMPORT_SCHEMA must be provided to osm2pgsql'
)
local import_fingerprint = assert(
    os.getenv('LIVABILITY_IMPORT_FINGERPRINT'),
    'LIVABILITY_IMPORT_FINGERPRINT must be provided to osm2pgsql'
)
local created_at = assert(
    os.getenv('LIVABILITY_IMPORT_CREATED_AT'),
    'LIVABILITY_IMPORT_CREATED_AT must be provided to osm2pgsql'
)

local healthcare_values = {
    pharmacy = true,
    hospital = true,
    clinic = true,
    doctors = true,
    dentist = true,
    health_centre = true,
}

local park_values = {
    park = true,
    playground = true,
    nature_reserve = true,
    garden = true,
}

local transport_rail_values = {
    station = true,
    tram_stop = true,
    halt = true,
}

local function feature_category(tags)
    if tags.shop then
        return 'shops'
    end
    if tags.highway == 'bus_stop' then
        return 'transport'
    end
    if tags.railway and transport_rail_values[tags.railway] then
        return 'transport'
    end
    if tags.amenity and healthcare_values[tags.amenity] then
        return 'healthcare'
    end
    if tags.leisure and park_values[tags.leisure] then
        return 'parks'
    end
    return nil
end

local features = osm2pgsql.define_table({
    name = 'features',
    schema = schema_name,
    ids = { type = 'any', id_column = 'osm_id', type_column = 'osm_type' },
    columns = {
        { column = 'import_fingerprint', type = 'text', not_null = true },
        { column = 'category', type = 'text', not_null = true },
        { column = 'name', type = 'text' },
        { column = 'tags_json', type = 'jsonb', not_null = true },
        { column = 'geom', type = 'geometry', projection = 4326, not_null = true },
        { column = 'created_at', sql_type = 'timestamptz', not_null = true },
    },
})

local function insert_feature(object, geom)
    local category = feature_category(object.tags)
    if not category or not geom then
        return
    end
    features:insert({
        import_fingerprint = import_fingerprint,
        category = category,
        name = object.tags.name,
        tags_json = object.tags,
        geom = geom,
        created_at = created_at,
    })
end

function osm2pgsql.process_node(object)
    local category = feature_category(object.tags)
    if not category then
        return
    end
    insert_feature(object, object:as_point())
end

function osm2pgsql.process_way(object)
    local category = feature_category(object.tags)
    if not category then
        return
    end

    local feature_geom = object:as_polygon()
    if not feature_geom then
        feature_geom = object:as_linestring()
    end
    insert_feature(object, feature_geom)
end

function osm2pgsql.process_relation(object)
    local category = feature_category(object.tags)
    if not category then
        return
    end
    local feature_geom = object:as_multipolygon()
    if feature_geom then
        insert_feature(object, feature_geom)
    end
end
