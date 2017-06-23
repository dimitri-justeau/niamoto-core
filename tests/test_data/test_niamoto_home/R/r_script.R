process <- function(){
    df1 <- get_occurrence_dataframe()
    df2 <- get_plot_dataframe()
    df3 <- get_plot_occurrence_dataframe()
    df4 <- get_taxon_dataframe()
    raster_str <- get_raster("test_raster")
    return(df1)
}
