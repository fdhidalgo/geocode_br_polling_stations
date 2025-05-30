## normalize addresses
normalize_address <- function(x) {
        str_to_lower(x) |>
                stringi::stri_trans_general("Latin-ASCII") |>
                str_remove_all("[[:punct:]]") |>
                str_remove("\\bzona rural\\b") |>
                str_remove("\\bpovoado\\b") |>
                str_remove("\\blocalidade\\b") |>
                str_remove_all("\\.|\\/") |>
                str_replace_all("^av\\b", "avenida") |>
                str_replace_all("^r\\b", "rua") |>
                str_replace_all("\\bs n\\b", "sn") |>
                # Following removes telephone numbers that are sometimes included
                str_remove("\\btel\\b.*") |>
                str_squish()
}



## normalize school names
normalize_school <- function(x) {
        ## School synonyms
        school_syns <- c(
                "e m e i", "esc inf", "esc mun", "unidade escolar", "centro educacional", "escola municipal",
                "colegio estadual", "cmei", "emeif", "emeief", "grupo escolar",
                "escola estadual", "erem", "colegio municipal",
                "centro de ensino infantil", "escola mul", "e m", "grupo municipal", "e e", "creche", "escola",
                "colegio", "em", "de referencia", "centro comunitario", "grupo", "de referencia em ensino medio",
                "intermediaria", "ginasio municipal", "ginasio", "emef", "centro de educacao infantil", "esc", "ee",
                "e f", "cei", "emei", "ensino fundamental", "ensino medio", "eeief", "eef", "e f", "ens fun",
                "eem", "eeem", "est ens med", "est ens fund", "ens fund", "mul", "professora", "professor",
                "eepg", "eemg", "prof", "ensino fundamental"
        )
        ## Normalize school names
        stringi::stri_trans_general(x, "Latin-ASCII") |>
                str_to_lower() |>
                str_remove_all("\\.") |>
                str_remove_all("[[:punct:]]") |>
                str_squish() |>
                str_remove_all(paste0("\\b", school_syns, "\\b", collapse = "|")) |> ## Remove School synonyms
                str_squish()
}


make_tract_centroids <- function(tracts) {
        ## Create tract centroids
        tracts$centroid <- sf::st_transform(tracts, 4674) |>
                sf::st_centroid() |>
                sf::st_geometry()
        tracts$tract_centroid_long <- sf::st_coordinates(tracts$centroid)[, 1]
        tracts$tract_centroid_lat <- sf::st_coordinates(tracts$centroid)[, 2]
        tracts <- sf::st_drop_geometry(tracts)
        tracts <- data.table(tracts)[, .(code_tract, zone, tract_centroid_lat, tract_centroid_long)]
        setnames(tracts, "code_tract", "setor_code")
        tracts
}

convert_coord <- function(coord) {
        # Function to convert a single coordinate to decimal degrees
        parts <- unlist(strsplit(coord, " "))
        degrees <- as.numeric(parts[1])
        minutes <- as.numeric(parts[2])
        seconds <- as.numeric(parts[3])
        direction <- gsub("[^NSWO]", "", parts[4])

        decimal_degrees <- degrees + (minutes / 60) + (seconds / 3600)

        if (direction %in% c("S", "W", "O")) {
                decimal_degrees <- -decimal_degrees
        }

        return(decimal_degrees)
}

clean_cnefe10 <- function(cnefe_file, muni_ids, tract_centroids) {
        ## This function cleans the CNEFE 2010 data
        cnefe <- fread(cnefe_file,
                drop = c(
                        "SITUACAO_SETOR", "NOM_COMP_ELEM1",
                        "VAL_COMP_ELEM1", "NOM_COMP_ELEM2",
                        "VAL_COMP_ELEM2", "NOM_COMP_ELEM3",
                        "VAL_COMP_ELEM3", "NOM_COMP_ELEM4",
                        "VAL_COMP_ELEM4", "NOM_COMP_ELEM5",
                        "VAL_COMP_ELEM5", "INDICADOR_ENDERECO",
                        "NUM_QUADRA", "NUM_FACE", "CEP_FACE",
                        "COD_UNICO_ENDERECO"
                )
        )
        ##

        setnames(
                cnefe, names(cnefe),
                tolower(names(cnefe))
        ) # make nicer names


        ## Pad administrative codes to proper length
        cnefe[, cod_municipio := str_pad(cod_municipio,
                width = 5, side = "left", pad = "0"
        )]
        cnefe[, cod_distrito := str_pad(cod_distrito,
                width = 2, side = "left", pad = "0"
        )]
        cnefe[, cod_setor := str_pad(cod_setor,
                width = 6, side = "left", pad = "0"
        )]
        cnefe[, setor_code := paste0(cod_uf, cod_municipio, cod_distrito, cod_setor)]
        cnefe[, c("cod_distrito", "cod_setor") := NULL]


        ## Create address variable
        cnefe[, num_endereco_char := fifelse(
                num_endereco == 0,
                dsc_modificador, as.character(num_endereco)
        )]
        cnefe[, dsc_modificador_nosn := fifelse(dsc_modificador != "SN", dsc_modificador, "")]
        cnefe[, address := str_squish(paste(
                nom_tipo_seglogr,
                nom_titulo_seglogr,
                nom_seglogr,
                num_endereco_char,
                dsc_modificador_nosn
        ))]
        cnefe[, street := str_squish(paste(
                nom_tipo_seglogr,
                nom_titulo_seglogr,
                nom_seglogr
        ))]
        cnefe[, c(
                "nom_tipo_seglogr", "nom_titulo_seglogr",
                "num_endereco_char", "num_endereco", "nom_seglogr",
                "dsc_modificador_nosn", "dsc_modificador"
        ) := NULL]

        ## ADD NAs where data is missing
        cnefe[val_longitude == "", val_longitude := NA]
        cnefe[val_latitude == "", val_latitude := NA]
        cnefe[dsc_estabelecimento == "", dsc_estabelecimento := NA]

        ## Remove extraneous white space from dsc_estabelecimento
        cnefe[, dsc_estabelecimento := str_squish(dsc_estabelecimento)]

        ## Add municipality code and identifiers

        cnefe[, id_munic_7 := as.numeric(paste0(cod_uf, cod_municipio))]
        cnefe <- muni_ids[, .(id_munic_7, id_TSE, municipio, estado_abrev)][cnefe, on = "id_munic_7"]

        gc()
        ## Merge in especie labels
        especie_labs <- data.table(
                especie = 1:7,
                especie_lab = c(
                        "domicílio particular", "domicílio coletivo",
                        "estabeleciemento agropecuário",
                        "estabelecimento de ensino",
                        "estabelecimento de saúde",
                        "estabeleciemento de outras finalidades",
                        "edificação em construção"
                )
        )
        cnefe <- especie_labs[cnefe, on = "especie"]


        ## Make smaller CNEFE dataset
        addr <- cnefe[, .(
                id_munic_7, id_TSE, municipio, setor_code, especie_lab, street,
                address, dsc_localidade, dsc_estabelecimento, val_longitude, val_latitude
        )]
        setnames(
                addr, c("dsc_localidade", "dsc_estabelecimento", "val_longitude", "val_latitude"),
                c("bairro", "desc", "cnefe_long", "cnefe_lat")
        )
        rm(cnefe)
        gc()

        # Convert degree longitude and latitude to decimal minutes
        addr[!is.na(cnefe_long), cnefe_long := sapply(cnefe_long, convert_coord)]
        addr[, cnefe_long := as.numeric(cnefe_long)]
        addr[!is.na(cnefe_lat), cnefe_lat := sapply(cnefe_lat, convert_coord)]
        addr[, cnefe_lat := as.numeric(cnefe_lat)]

        ## Imputation of longitude and latitude for CNEFE
        ## Merge in centroids
        addr <- tract_centroids[addr, on = "setor_code"]
        # Note that some setor codes in the CNEFE data appear to bad

        ## If longitude and latitude is missing, but tract centroid is available, use tract centroid
        addr$cnefe_impute_tract_centroid <- as.numeric(is.na(addr$cnefe_lat) &
                (is.na(addr$tract_centroid_lat) == FALSE))

        ## If longitude and latitude is missing and tract centroid is missing, drop
        addr <- addr[(is.na(addr$cnefe_lat) & (is.na(addr$tract_centroid_lat) == TRUE)) == FALSE]

        addr[cnefe_impute_tract_centroid == 1, cnefe_long := tract_centroid_long]
        addr[cnefe_impute_tract_centroid == 1, cnefe_lat := tract_centroid_lat]

        # Normalize name
        addr[, norm_address := normalize_address(address)]
        addr[, norm_bairro := normalize_address(bairro)]
        addr[, norm_street := normalize_address(street)]

        addr
}

clean_cnefe22 <- function(cnefe22_file, muni_ids) {
        ## This function cleans the CNEFE 2022 data
        cnefe22 <- fread(cnefe22_file,
                drop = c(
                        "NOM_COMP_ELEM1",
                        "VAL_COMP_ELEM1", "NOM_COMP_ELEM2",
                        "VAL_COMP_ELEM2", "NOM_COMP_ELEM3",
                        "VAL_COMP_ELEM3", "NOM_COMP_ELEM4",
                        "VAL_COMP_ELEM4", "NOM_COMP_ELEM5",
                        "VAL_COMP_ELEM5", "NUM_QUADRA", "NUM_FACE",
                        "COD_UNICO_ENDERECO"
                )
        )

        setnames(
                cnefe22, names(cnefe22),
                tolower(names(cnefe22))
        ) # make nicer names

        ## Create address variable
        cnefe22[, num_endereco_char := fifelse(
                num_endereco == 0,
                dsc_modificador, as.character(num_endereco)
        )]
        ## Remove SN designation
        cnefe22[, num_endereco_char := str_remove(num_endereco_char, "SN")]
        cnefe22[, dsc_modificador_nosn := fifelse(dsc_modificador != "SN", dsc_modificador, "")]

        cnefe22[, address := str_squish(paste(
                nom_tipo_seglogr,
                nom_titulo_seglogr,
                nom_seglogr,
                num_endereco_char,
                dsc_modificador_nosn
        ))]
        cnefe22[, street := str_squish(paste(
                nom_tipo_seglogr,
                nom_titulo_seglogr,
                nom_seglogr
        ))]
        cnefe22[, c(
                "nom_tipo_seglogr", "nom_titulo_seglogr",
                "num_endereco_char", "num_endereco", "nom_seglogr",
                "dsc_modificador_nosn", "dsc_modificador"
        ) := NULL]

        ## ADD NAs where data is missing
        cnefe22[dsc_estabelecimento == "", dsc_estabelecimento := NA]
        ## Remove extraneous white space from dsc_estabelecimento
        cnefe22[, dsc_estabelecimento := str_squish(dsc_estabelecimento)]

        setnames(cnefe22, "cod_municipio", "id_munic_7")
        cnefe22 <- merge(
                cnefe22,
                muni_ids[, .(id_munic_7, id_TSE, municipio, estado_abrev)],
                by.x = "id_munic_7",
                by.y = "id_munic_7",
                all.x = TRUE
        )
        ## Merge in especie labels
        especie_labs <- data.table(
                cod_especie = 1:8,
                especie_lab = c(
                        "domicílio particular", "domicílio coletivo",
                        "estabeleciemento agropecuário",
                        "estabelecimento de ensino",
                        "estabelecimento de saúde",
                        "estabeleciemento de outras finalidades",
                        "edificação em construção ou reforma",
                        "estabeleciemento religioso"
                )
        )

        cnefe22 <- merge(
                cnefe22,
                especie_labs,
                by = "cod_especie",
                all.x = TRUE
        )

        ## Merge in geocoding labels
        nv_geo_coord_labs <- data.table(
                nv_geo_coord = 1:6,
                nv_geo_coord_lab = c(
                        "Endereço - coordenada original do Censo 2022",
                        "Endereço - coordenada modificada (apartamentos em um mesmo número no logradouro)",
                        "Endereço - coordenada estimada (endereços originalmente sem coordenadas ou coordenadas inválidas)",
                        "Face de quadra",
                        "Localidade",
                        "Setor censitário"
                )
        )

        cnefe22 <- merge(
                cnefe22,
                nv_geo_coord_labs,
                by = "nv_geo_coord",
                all.x = TRUE
        )

        cnefe22 <- cnefe22[, .(
                id_munic_7, id_TSE, municipio, cod_setor, especie_lab, street,
                address, dsc_localidade, dsc_estabelecimento, longitude, latitude,
                nv_geo_coord_lab
        )]

        setnames(
                cnefe22, c("dsc_localidade", "dsc_estabelecimento", "longitude", "latitude"),
                c("bairro", "desc", "cnefe_long", "cnefe_lat")
        )

        # Normalize name
        cnefe22[, norm_address := tolower(address)]
        cnefe22[, norm_bairro := tolower(bairro)]
        cnefe22[, norm_street := tolower(street)]
        cnefe22
}

get_cnefe22_schools <- function(cnefe22) {
        ## This function extracts schools from the CNEFE 2022 data
        schools_cnefe22 <- cnefe22[especie_lab == "estabelecimento de ensino"]
        schools_cnefe22[, norm_desc := normalize_school(desc)]
        schools_cnefe22[norm_desc != ""]
}


clean_inep <- function(inep_data, inep_codes) {
        ## This function cleans the INEP school data
        setnames(
                inep_data, names(inep_data),
                str_replace_all(stringi::stri_trans_general(tolower(names(inep_data)), "Latin-ASCII"), " ", "_")
        )
        ## remove diacritics from names
        inep_data <- inep_data[!is.na(latitude), .(
                escola, codigo_inep, uf,
                municipio, endereco, latitude, longitude
        )]
        inep_data <- inep_codes[inep_data, on = "codigo_inep"]

        # normalize name and address
        inep_data[, norm_school := normalize_school(escola)]
        inep_data[, norm_addr := normalize_address(endereco)]
        # remove cep and municipality
        inep_data[, norm_addr := str_remove(norm_addr, " ([0-9]{5}).*")]
        inep_data
}
#
#
clean_tsegeocoded_locais <- function(tse_files, muni_ids, locais) {
        # Read the data from the 2018, 2020, and 2022 election files
        loc18 <- fread(tse_files[1],
                encoding = "Latin-1"
        )
        loc20 <- fread(tse_files[2],
                encoding = "Latin-1"
        )
        loc22 <- fread(tse_files[3],
                encoding = "Latin-1"
        )

        # Combine the data from all three years into a single data frame
        locs <- rbindlist(list(loc18, loc20, loc22), fill = TRUE)

        # Only keep columns that are present in all data sets
        locs <- locs[, names(loc22)[names(loc22) %in% names(loc18) == TRUE], with = FALSE]

        # Convert column names to lowercase
        setnames(locs, names(locs), tolower(names(locs)))

        # Remove duplicate rows and filter out polling stations out of the country
        locs <- unique(locs[sg_uf != "ZZ", .(
                aa_eleicao, sg_uf, cd_municipio, nm_municipio, nr_zona,
                nr_local_votacao, nm_local_votacao,
                ds_endereco, nm_bairro, nr_cep, nr_latitude, nr_longitude
        )])

        # Set 'nr_latitude' and 'nr_longitude' to NA if they are equal to -1
        locs[, nr_latitude := ifelse(nr_latitude == -1, NA, nr_latitude)]
        locs[, nr_longitude := ifelse(nr_longitude == -1, NA, nr_longitude)]

        # Remove rows with NA values in 'nr_latitude'
        locs <- locs[!is.na(nr_latitude)]

        # Merge 'locs' with 'muni_ids' based on 'cd_municipio' and 'id_TSE'
        locs <- merge(locs, muni_ids[, .(id_munic_7, id_TSE)],
                by.x = c("cd_municipio"),
                by.y = c("id_TSE"), all.x = TRUE
        )

        # Rename columns in 'locs'
        setnames(
                locs, c("aa_eleicao", "id_munic_7", "nr_local_votacao", "nr_latitude", "nr_longitude"),
                c("ano", "cod_localidade_ibge", "nr_locvot", "tse_lat", "tse_long")
        )

        # Merge 'locs' with 'locais' based on 'ano', 'cod_localidade_ibge', 'nr_zona', and 'nr_locvot'
        locs <- merge(locs, locais[, .(
                local_id, ano, cod_localidade_ibge,
                nr_zona, nr_locvot
        )],
        all.x = TRUE, all.y = FALSE,
        by = c("ano", "cod_localidade_ibge", "nr_zona", "nr_locvot")
        )

        # Remove rows with NA values in 'local_id'
        locs <- locs[!is.na(local_id)]

        # Group by 'local_id' and keep only the most recent year
        locs <- locs[order(-ano), .SD[1], by = local_id]

        locs
}


clean_agro_cnefe <- function(agro_cnefe_files, muni_ids) {
        ## This function cleans the 2017 CNEFE Data (agricultural census)
        agro_cnefe <-
                rbindlist(lapply(agro_cnefe_files, fread,
                        drop = c(
                                "SITUACAO", "NOM_COMP_ELEM1", "COD_DISTRITO",
                                "VAL_COMP_ELEM1", "NOM_COMP_ELEM2",
                                "VAL_COMP_ELEM2", "NOM_COMP_ELEM3",
                                "VAL_COMP_ELEM3", "NOM_COMP_ELEM4",
                                "VAL_COMP_ELEM4", "NOM_COMP_ELEM5",
                                "VAL_COMP_ELEM5", "ALTITUDE", "COD_ESPECIE",
                                "CEP"
                        )
                ))

        setnames(
                agro_cnefe, names(agro_cnefe),
                tolower(names(agro_cnefe))
        )
        ## Pad administrative codes to proper length
        agro_cnefe[, cod_municipio := str_pad(cod_municipio,
                width = 5, side = "left", pad = "0"
        )]

        ## Create address variable
        agro_cnefe[, num_endereco_chr := fifelse(
                is.na(num_endereco), "",
                as.character(num_endereco)
        )]

        agro_cnefe[, address := str_squish(paste(
                nom_tipo_seglogr,
                nom_titulo_seglogr,
                nom_seglogr,
                num_endereco_chr,
                dsc_modificador
        ))]
        agro_cnefe[, street := str_squish(paste(
                nom_tipo_seglogr,
                nom_titulo_seglogr,
                nom_seglogr
        ))]
        agro_cnefe[, c(
                "nom_tipo_seglogr", "nom_titulo_seglogr",
                "num_endereco_chr", "num_endereco", "nom_seglogr",
                "dsc_modificador"
        ) := NULL]

        ## Remove extraneous white space from dsc_localidade
        agro_cnefe[, dsc_localidade := str_squish(dsc_localidade)]

        ## Add municipality code and identifiers
        agro_cnefe[, id_munic_7 := as.numeric(paste0(cod_uf, cod_municipio))]
        agro_cnefe <- muni_ids[, .(id_munic_7, id_TSE, municipio, estado_abrev)][agro_cnefe, on = "id_munic_7"]

        agro_cnefe[, c("cod_uf", "cod_subdistrito") := NULL]

        agro_cnefe[, .(n = .N), by = dsc_localidade][order(-n)]

        # Normalize name
        agro_cnefe[, norm_address := normalize_address(address)]
        agro_cnefe[, norm_bairro := normalize_address(dsc_localidade)]
        agro_cnefe[, norm_street := normalize_address(street)]

        agro_cnefe
}
#
#

#
calc_muni_area <- function(muni_shp) {
        ## Calculate the area of each municipality
        area <- sf::st_area(muni_shp)
        muni_shp$area <- area
        muni_shp <- sf::st_drop_geometry(muni_shp)
        setDT(muni_shp)
        muni_shp[, .(cod_localidade_ibge = code_muni, area)]
}
#
import_locais <- function(locais_file, muni_ids) {
        ## This function imports and cleans the polling station data
        # Load the data
        locais_data <- fread(locais_file)

        # Clean column names
        setnames(locais_data, janitor::make_clean_names(names(locais_data)))

        # Replace NA values with empty strings
        locais_data[, ds_bairro := ifelse(is.na(ds_bairro), "", ds_bairro)]
        locais_data[, ds_endereco := ifelse(is.na(ds_endereco), "", ds_endereco)]

        # Normalize and mutate columns
        locais_data[, normalized_name := normalize_school(nm_locvot)]
        locais_data[, normalized_addr := paste(normalize_address(ds_endereco), normalize_address(ds_bairro))]
        locais_data[, normalized_st := normalize_address(ds_endereco)]
        locais_data[, normalized_bairro := normalize_address(ds_bairro)]

        # Load muni_ids and merge
        locais_data <- merge(locais_data,
                muni_ids[, .(cod_localidade_ibge = id_munic_7, cd_localidade_tse = id_TSE)],
                by = "cd_localidade_tse", all.x = TRUE
        )

        ## Remove polling stations abroad
        locais_data <- locais_data[sg_uf != "ZZ"]

        # Filter and add local_id
        # locais_data <- locais_data[!is.na(nm_locvot)]
        locais_data[, local_id := .I]
        locais_data
}

finalize_coords <- function(locais, model_predictions, tsegeocoded_locais) {
        ## order within local_id by pred_dist and pick the first one in each group
        best_match <- model_predictions[
                order(local_id, pred_dist),
                .(local_id, long, lat, pred_dist)
        ]
        best_match <- best_match[, .SD[1],
                by = local_id
        ]

        geocoded_locais <- merge(
                locais,
                best_match,
                by = "local_id",
                all.x = TRUE
        )
        geocoded_locais[, c("normalized_name", "normalized_addr", "normalized_st", "normalized_bairro") := NULL]
        setnames(geocoded_locais, c("long", "lat"), c("pred_long", "pred_lat"))
        geocoded_locais <- merge(
                geocoded_locais,
                tsegeocoded_locais[, .(local_id, tse_lat, tse_long)],
                by = "local_id",
                all.x = TRUE
        )
        ### if we have ground truth distance from TSE, then assign ground truth
        geocoded_locais[, long := ifelse(!is.na(tse_long), tse_long, pred_long)]
        geocoded_locais[, lat := ifelse(!is.na(tse_lat), tse_lat, pred_lat)]
        geocoded_locais[, pred_dist := ifelse(!is.na(tse_long), 0, pred_dist)]

        geocoded_locais <- geocoded_locais[, c(
                "local_id", "ano", "sg_uf", "cd_localidade_tse", "cod_localidade_ibge",
                "nr_zona", "nr_locvot", "nr_cep", "nm_localidade", "nm_locvot",
                "ds_endereco", "ds_bairro", "pred_long", "pred_lat", "pred_dist",
                "tse_long", "tse_lat", "long", "lat"
        )]

        geocoded_locais
}
#
export_geocoded_locais <- function(geocoded_locais) {
        fwrite(geocoded_locais, "./output/geocoded_polling_stations.csv.gz")
        "./output/geocoded_polling_stations.csv.gz"
}
