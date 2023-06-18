
# load packages -----------------------------------------------------------

library(aws.s3)
library(tidyverse)
library(arrow)
library(sf)
library(geodata)
library(slippymath)
library(hrbrthemes)
library(showtext)
library(tidytext)


# setting plot theme ------------------------------------------------------

font_add_google("Roboto")

showtext_auto()

theme_set(
  theme_ipsum(
    base_family = "Roboto",
    grid = "XY"
  ) +
    theme(
      plot.title.position = "plot",
      plot.caption.position = "plot",
      legend.position = "bottom",
      plot.background = element_rect(fill = "#e5e5e3", color = NA),
      panel.background = element_rect(fill = "#e5e5e3", color = NA)
    )
)

# Define helper function --------------------------------------------------

as_binary = function(x){
  tmp = rev(as.integer(intToBits(x)))
  id = seq_len(match(1, tmp, length(tmp)) - 1)
  tmp[-id]
}

deg2num = function(lat_deg, lon_deg, zoom) {
  lat_rad = lat_deg * pi /180
  n = 2.0 ^ zoom
  xtile = floor((lon_deg + 180.0) / 360.0 * n)
  ytile = floor((1.0 - log(tan(lat_rad) + (1 / cos(lat_rad))) / pi) / 2.0 * n)
  c(xtile, ytile)
}

# reference JavaScript implementations
# https://developer.here.com/documentation/traffic/dev_guide/common/map_tile/topics/quadkeys.html

tileXYToQuadKey = function(xTile, yTile, z) {
  quadKey = ""
  for (i in z:1) {
    digit = 0
    mask = bitwShiftL(1, i - 1)
    xtest = as_binary(bitwAnd(xTile, mask))
    if(any(xtest)) {
      digit = digit + 1
    }
    
    ytest = as_binary(bitwAnd(yTile, mask))
    if(any(ytest)) {
      digit = digit + 2
    }
    quadKey = paste0(quadKey, digit)
  }
  quadKey
}

get_perf_tiles <- function(bbox, tiles){
  bbox <- st_bbox(
    st_transform(
      st_as_sfc(bbox),
      4326
    ))
  tile_grid <- bbox_to_tile_grid(bbox, zoom = 16)
  # zoom level 16 held constant, otherwise loop through the tile coordinates calculated above
  quadkeys <- pmap(list(tile_grid$tiles$x, tile_grid$tiles$y, 16), tileXYToQuadKey)
  perf_tiles <- tiles %>%
    filter(quadkey %in% quadkeys)
}

# Download data -----------------------------------------------------------

bucket_exists(
  bucket = "s3://ookla-open-data/",
  region = "us-west-2"
              )


ookla_bucket <-
get_bucket_df(
  bucket = "s3://ookla-open-data/",
  region = "us-west-2",
  max = 10000
) %>%
  as_tibble()

ookla_objects <-
ookla_bucket %>%
  filter(str_detect(Key, "year=2023")) %>%
  filter(str_detect(Key, "quarter=1")) %>%
  filter(str_detect(Key, "parquet")) %>%
  pull(Key)


for (ookla_object in ookla_objects) {
  message("Downloading", basename(ookla_object))
  save_object(
    object = ookla_objects[[1]],
    bucket = "s3://ookla-open-data/",
    region = "us-west-2",
    file = file.path("data-raw", basename(ookla_object)),
    overwrite = FALSE
  )
  message("Done")
}


# import data -------------------------------------------------------------

filepaths <-
list.files(
  "data-raw", 
  pattern = "parquet",
  full.names = TRUE
  ) %>%
  set_names(., nm = str_extract(., "(fixed|mobile)_tiles"))


fixed_tiles_raw <-
  read_parquet(file = filepaths[["fixed_tiles"]])

dim(fixed_tiles_raw)
colnames(fixed_tiles_raw)


# Preprocess data ---------------------------------------------------------

#' 1. Define boundary area (e.g. cities)

idn_basemap <-
  gadm(country = "idn", level = 2, path = "data-raw") %>%
  st_as_sf(crs = 4326)

class(idn_basemap)

depok_basemap <-
idn_basemap %>%
  filter(NAME_2 == "Depok")


depok_basemap_detailed <-
  gadm(country = "idn", level = 3, path = "data-raw") %>%
  st_as_sf(crs = 4326) %>%
  filter(NAME_2 == "Depok")

plot(depok_basemap)

#' 2. Calculate bounding box

depok_bbox <- st_bbox(depok_basemap)
depok_bbox

#' 3. Filter tiles data using quadkey-bounding box

depok_fixed_tiles_raw <- 
  get_perf_tiles(depok_bbox, fixed_tiles_raw) %>%
  st_as_sf(wkt = "tile", crs = 4326)

depok_fixed_tiles_raw

ggplot() +
  geom_sf(
    data = depok_fixed_tiles_raw
  ) +
  geom_sf(
    data = depok_basemap,
    fill = "grey",
    alpha = 0.75
  )

#' 4. Refine the tiles

depok_fixed_tiles <-
  depok_fixed_tiles_raw %>%
  st_join(
    depok_basemap,
    join = st_intersects,
    left = FALSE
  )

ggplot() +
  geom_sf(
    data = depok_fixed_tiles
  ) +
  geom_sf(
    data = depok_basemap,
    fill = "grey",
    alpha = 0.5
  )



for (filepath in filepaths) {
  
  if (!dir.exists("data"))  {
    dir.create("data")
  }
  
  tiles_raw <-
    read_parquet(file = filepath)
  depok_tiles <- 
    get_perf_tiles(depok_bbox, tiles_raw) %>%
    st_as_sf(wkt = "tile", crs = 4326) %>%
    st_join(
      depok_basemap,
      join = st_intersects,
      left = FALSE
    )
  write_rds(
    x = depok_tiles,
    file = file.path("data", paste0("depok--", tools::file_path_sans_ext(basename(filepath)), ".rds")),
    compress = "bz"
  )
  
}


# Exploratory analysis & viz ----------------------------------------------


depok_mobile_tiles <- read_rds("data/depok--2022-07-01_performance_mobile_tiles.rds")

depok_mobile_tiles

ggplot() +
  geom_sf(
    data = depok_mobile_tiles,
    aes(fill = avg_d_kbps / 1000),
    colour = NA
  ) +
  geom_sf(
    data = depok_basemap,
    fill = NA,
    colour = 'gold'
  ) +
  scale_fill_viridis_c(
    name = "Avg download speed (Mbps)",
    option = "plasma",
    guide = guide_colourbar(
      title.position = "bottom",
      barwidth = unit(0.6, "npc"),
      barheight = unit(0.025, "npc")
    )
  ) +
  labs(
    title = "Internet performance in Depok, West Java",
    subtitle = " Data of mobile internet network, Q3 2022",
    caption = "Source: Ookla's open data initiative"
  )


depok_mobile_tiles_detailed <-
  depok_mobile_tiles %>%
  st_join(depok_basemap_detailed)

plot(depok_mobile_tiles_detailed['NAME_3'])



ggplot() +
  geom_sf(
    data = depok_mobile_tiles_detailed,
    aes(fill = avg_d_kbps / 1000),
    colour = NA
  ) +
  geom_sf(
    data = depok_mobile_tiles_detailed,
    fill = NA,
    colour = 'gold'
  ) +
  facet_wrap(~NAME_3) +
  scale_fill_viridis_c(
    name = "Avg download speed (Mbps)",
    option = "plasma",
    guide = guide_colourbar(
      title.position = "bottom",
      barwidth = unit(0.6, "npc"),
      barheight = unit(0.025, "npc")
    )
  ) +
  labs(
    title = "Internet performance in Depok, West Java",
    subtitle = " Data of mobile internet network, Q3 2022",
    caption = "Source: Ookla's open data initiative"
  )

depok_mobile_tiles_detailed_summary <-
  depok_mobile_tiles_detailed %>%
  st_drop_geometry() %>%
  group_by(
    subdistrict = NAME_3
  ) %>%
  summarise(
    download = weighted.mean(avg_d_kbps, tests) / 1000,
    upload = weighted.mean(avg_u_kbps, tests) / 1000
  )

depok_mobile_tiles_detailed_summary %>%
  mutate(
    subdistrict = fct_reorder(subdistrict, download)
  ) %>%
  ggplot(aes(download, subdistrict)) +
  geom_col(fill = "steelblue")


depok_mobile_tiles_detailed_summary %>%
  pivot_longer(
    cols = -subdistrict
  ) %>%
  mutate(
    subdistrict = reorder_within(subdistrict, value, name)
  ) %>%
  ggplot(aes(value, subdistrict)) +
  facet_wrap(~name, scales = "free_y") +
  geom_col(aes(fill = name), show.legend = FALSE) +
  scale_y_reordered() +
  scale_fill_manual(
    values = c(
      "download" = "steelblue",
      "upload" = "salmon"
    )
  )
