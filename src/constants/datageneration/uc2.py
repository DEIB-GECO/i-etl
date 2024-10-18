ETHNICITIES = ["ASH", "NA", "O", "Y", "Et", "S", "B", "BU", "I", "IS", "M", "MR", "G", "Gr", "T", "Eg", "U", "AM", "B",
               "AC"]

FUNDUS_DESCRIPTIONS = [
    "The optic disc is round with sharp margins and a pinkish hue. The cup-to-disc ratio is about 0.3. The retinal vessels have a regular caliber with no signs of narrowing or tortuosity. The macula appears flat, with a well-defined foveal reflex. The retinal background is orange-red without hemorrhages, exudates, or pigmentary changes.",
    "The optic disc margins are clear. Multiple microaneurysms, dot and blot hemorrhages, and scattered hard exudates are present in all quadrants. The macula shows mild retinal thickening and some exudates near the fovea, indicating macular edema. No neovascularization is noted.",
    "The optic disc margins are clear. There is generalized arteriolar narrowing, with arteriovenous nicking observed at several points. Flame-shaped hemorrhages and cotton-wool spots are seen in the superior and inferior temporal quadrants. No optic disc edema is present.",
    "The optic disc is normal in appearance. Multiple small and large drusen are present in the macular area. The retinal pigment epithelium shows areas of hyperpigmentation and atrophy. No signs of subretinal fluid or hemorrhage are seen.",
    "The optic disc is hyperemic and blurred. Widespread retinal hemorrhages are seen in all quadrants. Venous dilation and tortuosity are marked. Cotton-wool spots are scattered throughout the retina. Macular edema is present.",
    "The optic disc is swollen, with blurred and elevated margins. The cup is obscured due to the swelling. Venous congestion is noted, with dilated and tortuous veins. Peripapillary hemorrhages and cotton-wool spots are visible. The retinal background is normal apart from changes near the optic disc.",
    "The optic disc is normal. There is a grayish, elevated retinal fold in the superior temporal quadrant with a demarcation line. The detached retina appears undulating with visible retinal vessels. No tears or holes are directly visible, but the macula remains attached.",
    "The optic disc and retinal vessels appear normal. There is a dome-shaped, pigmented lesion in the inferior nasal quadrant. The lesion has irregular borders with associated retinal detachment. Overlying orange pigment and drusen are visible on the surface of the lesion.",
    "The optic disc appears pale with waxy atrophy. Arterioles are attenuated throughout the retina. There is bone-spicule pigmentation in the mid-peripheral retina. The macula appears normal, though the foveal reflex may be absent.",
    "The optic disc is normal. There is a well-demarcated, circular area of retinal elevation in the macular region, indicating subretinal fluid. The retinal pigment epithelium shows subtle changes, including pigment clumping and mottling. No hemorrhages, exudates, or signs of choroidal neovascularization are present."
]

WIDE_FIELD_COLOR = [
    "The optic disc is well-defined with a pinkish hue and a cup-to-disc ratio of 0.3. Retinal vessels have a regular caliber without signs of tortuosity or narrowing. The macula shows a normal foveal reflex, and the peripheral retina appears uniform with no lesions, hemorrhages, or pigmentary changes.",
    "Multiple dot and blot hemorrhages are present throughout the retina, most notably in the mid-periphery. Microaneurysms are scattered, along with patches of hard exudates. No neovascularization is observed. The optic disc margins are clear.",
    "The retinal arterioles are narrowed with areas of arteriovenous (AV) nicking. Cotton-wool spots and flame-shaped hemorrhages are visible in the superior and inferior temporal quadrants. The optic disc is normal, and the macula appears intact.",
    "A grayish, elevated retinal area is seen in the inferior temporal quadrant. The detached retina appears undulating with prominent retinal vessels. No visible retinal tears, but the optic disc and macula remain attached.",
    " The optic disc is hyperemic and obscured by hemorrhages. Diffuse, deep, and superficial retinal hemorrhages are present in all quadrants. The veins are dilated and tortuous."
    "The macula has a reddish-orange discoloration with subretinal hemorrhage and grayish-green choroidal neovascular membrane. The peripheral retina appears normal. Drusen and retinal pigment epithelial changes are noted near the macula.",
    "The optic disc appears pale with a waxy atrophy. The retinal vessels, particularly the arterioles, are markedly attenuated. Bone-spicule pigmentation is scattered throughout the mid-peripheral retina. The macula shows mild retinal pigment epithelial changes.",
    "The macula displays a round, well-circumscribed defect. There is no evidence of vitreomacular traction. The optic disc and the peripheral retina are unremarkable.",
    "A dome-shaped, pigmented lesion is seen in the superior nasal quadrant. The lesion has irregular borders, and there is associated retinal detachment around its margins. Overlying drusen and orange pigment can be observed.",
    "The superior temporal quadrant shows flame-shaped and blot hemorrhages confined to the distribution of the affected vessel. There is retinal edema and a few cotton-wool spots in the area of the occlusion. The optic disc and other quadrants appear normal."
]

WIDE_FIELD_AUTOFLUORESCENCE = [
    "The optic disc appears dark and non-autofluorescent. Retinal blood vessels are also dark, blocking the underlying fluorescence. The background retinal autofluorescence is relatively uniform with a slight decrease in the macular region due to macular pigments. No areas of abnormal hyper- or hypofluorescence are seen.",
    "Multiple areas of scattered hyperautofluorescence corresponding to drusen are present in the macular area. Surrounding these areas, there are patches of hypoautofluorescence indicating retinal pigment epithelium (RPE) atrophy. Some areas show a mottled pattern of mixed hyper- and hypoautofluorescence, suggestive of varying stages of RPE damage.",
    "There is a classic ring of hyperautofluorescence surrounding the macula, representing zones of degenerating RPE. Beyond this ring, extensive areas of hypoautofluorescence are seen in the mid-peripheral retina, indicating widespread RPE atrophy. The optic disc and retinal vessels show normal autofluorescence patterns.",
    "Focal areas of increased hyperautofluorescence are noted in the macular region, suggesting the accumulation of subretinal fluid or focal RPE changes. There may also be associated areas of granular hypoautofluorescence around the zones of leakage, indicating chronic RPE alterations.",
    "The macular region shows a diffuse pattern of mottled hyper- and hypoautofluorescence. The hypoautofluorescent areas correspond to RPE atrophy, while the flecks of hyperautofluorescence represent lipofuscin accumulation in the RPE cells. Some areas of the peripheral retina may remain relatively normal.",
    "Extensive areas of hypoautofluorescence in the peripheral retina represent choroidal and RPE atrophy. The macula might remain relatively spared, showing normal autofluorescence, while the optic disc also appears dark. A few patches of residual hyperautofluorescence may indicate surviving RPE islands.",
    "The macula shows a well-defined area of intense hyperautofluorescence, representing the vitelliform lesion. Surrounding this, there might be scattered spots of hypoautofluorescence if there is any RPE atrophy. The peripheral retina typically appears normal.",
    "Large patches of hypoautofluorescence in the macular region correspond to areas of geographic atrophy. Surrounding these patches, a rim of hyperautofluorescence may indicate zones of ongoing RPE stress and degeneration. The peripheral retina usually maintains a normal autofluorescence pattern.",
    "A characteristic ring of hyperautofluorescence surrounds the macula, resembling a bull's-eye pattern. The central macular region shows hypoautofluorescence, indicating RPE and photoreceptor cell damage. The peripheral retina typically appears unaffected.",
    "The macula demonstrates a large, central area of hypoautofluorescence, indicating cone cell and RPE atrophy. Surrounding this, there may be a ring of hyperautofluorescence, representing stressed RPE. The peripheral retina might show scattered patches of hypoautofluorescence if the rod cells are also affected."
]

CENTRAL_OPTICAL_COHERENCE = [
    "The retinal layers are well-defined, with a normal foveal depression. The retinal pigment epithelium (RPE) appears intact, and there is no evidence of subretinal fluid, intraretinal cysts, or macular thickening. Central retinal thickness is within the normal range (250–300 µm).",
    "The scan shows multiple cystic spaces within the inner retinal layers, particularly in the macular region. There is an increased central retinal thickness, often exceeding 300 µm. The cystoid spaces may appear as round or oval hyporeflective (dark) areas.",
    "A full-thickness defect is present in the central foveal area, often with a surrounding cuff of subretinal fluid. The edges of the retinal tissue may appear elevated. There is a loss of the normal foveal contour. In some cases, an operculum (a detached piece of retinal tissue) may be seen within the vitreous cavity.",
    "The OCT scan shows a hyperreflective membrane on the inner surface of the retina, causing retinal folds and distortion. The retinal layers beneath the membrane may appear thickened, and the foveal contour is typically disturbed.",
    "The OCT shows multiple small, hyperreflective deposits beneath the RPE in the macular area. The RPE layer appears undulating due to the presence of drusen. There is no evidence of subretinal fluid or intraretinal cysts.",
    "The scan reveals a hyperreflective area corresponding to neovascular membranes. There may be subretinal or intraretinal fluid, indicating leakage. The RPE may appear disrupted or elevated. Increased retinal thickness and cystoid spaces might be present in the macular region.",
    "A dome-shaped detachment of the neurosensory retina is visible in the macular area. There is clear subretinal fluid accumulation without significant retinal thickening. The RPE layer may appear altered or disrupted in the region of the detachment.",
    "The scan shows diffuse retinal thickening with multiple hyporeflective (dark) cystoid spaces within the retina, typically in the inner nuclear and outer plexiform layers. The central retinal thickness is often increased beyond the normal range. Subretinal fluid may also be present.",
    " The vitreous appears partially attached to the macula, exerting traction on the retinal surface. The retinal contour is distorted, often with a stretched or elevated foveal area. There may be cystoid spaces indicating early cystoid macular edema.",
    "The OCT shows areas of RPE thinning and atrophy, often with an abrupt loss of retinal layers. The choroidal structures may appear more prominent due to the loss of overlying retinal tissue. There is no evidence of subretinal fluid or cystoid changes."
]

COMORBIDITIES = ["Hypertension", "Diabetes Mellitus", "Obesity", "Hyperlipidemia",
                     "Chronic Obstructive Pulmonary Disease", "Coronary Artery Disease",
                     "Chronic Kidney Disease", "Arthritis", "Asthma"]

INHERITENCE = ["AR", "AD", "XL"]
