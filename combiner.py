def combine2020(geo, seg1, seg2, seg3):
    link_cols = ["FILEID", "STUSAB", "LOGRECNO"]
    combined_df_2020 = geo.join(seg1, link_cols).join(seg2, link_cols).join(seg3, link_cols)

    return combined_df_2020

def combine2000(geo, seg1, seg2):
    link_cols = ["FILEID", "STUSAB", "LOGRECNO"]
    combined_df_2000 = geo.join(seg1, link_cols).join(seg2, link_cols)

    return combined_df_2000