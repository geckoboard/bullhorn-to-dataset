package bullhorn

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"gotest.tools/v3/assert"
)

func TestPlacementService_Search(t *testing.T) {
	t.Run("returns placements", func(t *testing.T) {
		want := Placements{
			Items: []Placement{
				{
					ID: 1,

					DateAdded:        EpochMilli(1659190221000),
					DateBegin:        EpochMilli(1659290221000),
					DateEnd:          EpochMilli(1659990221000),
					DateLastModified: EpochMilli(1659193221000),

					CustomDate1:  EpochMilli(0),
					CustomDate2:  EpochMilli(1659194221000),
					CustomDate3:  EpochMilli(1659195221000),
					CustomDate4:  EpochMilli(1659196221000),
					CustomDate5:  EpochMilli(1659197221000),
					CustomDate6:  EpochMilli(1659198221000),
					CustomDate7:  EpochMilli(1659199221000),
					CustomDate8:  EpochMilli(1659200221000),
					CustomDate9:  EpochMilli(1659201221000),
					CustomDate10: EpochMilli(1659202221000),
					CustomDate11: EpochMilli(1659203221000),
					CustomDate12: EpochMilli(1659204221000),
					CustomDate13: EpochMilli(1659205221000),

					CustomText1:  "",
					CustomText2:  "text2",
					CustomText3:  "text3",
					CustomText4:  "text4",
					CustomText5:  "text5",
					CustomText6:  "text6",
					CustomText7:  "text7",
					CustomText8:  "text8",
					CustomText9:  "text9",
					CustomText10: "text10",
					CustomText11: "text11",
					CustomText12: "text12",
					CustomText13: "text13",
					CustomText14: "text14",
					CustomText15: "text15",
					CustomText16: "text16",
					CustomText17: "text17",
					CustomText18: "text18",
					CustomText19: "text19",
					CustomText20: "text20",
					CustomText21: "text21",
					CustomText22: "text22",
					CustomText23: "text23",
					CustomText24: "text24",
					CustomText25: "text25",
					CustomText26: "text26",
					CustomText27: "text27",
					CustomText28: "text28",
					CustomText29: "text29",
					CustomText30: "text30",
					CustomText31: "text31",
					CustomText32: "text32",
					CustomText33: "text33",
					CustomText34: "text34",
					CustomText35: "text35",
					CustomText36: "text36",
					CustomText37: "text37",
					CustomText38: "text38",
					CustomText39: "text39",
					CustomText40: "text40",
					CustomText41: "text41",
					CustomText42: "text42",
					CustomText43: "text43",
					CustomText44: "text44",
					CustomText45: "text45",
					CustomText46: "text46",
					CustomText47: "text47",
					CustomText48: "text48",
					CustomText49: "text49",
					CustomText50: "text50",
					CustomText51: "text51",
					CustomText52: "text52",
					CustomText53: "text53",
					CustomText54: "text54",
					CustomText55: "text55",
					CustomText56: "text56",
					CustomText57: "text57",
					CustomText58: "text58",
					CustomText59: "text59",
					CustomText60: "text60",

					// Custom float fields
					CustomFloat1:  0,
					CustomFloat2:  2,
					CustomFloat3:  3,
					CustomFloat4:  4,
					CustomFloat5:  5,
					CustomFloat6:  6,
					CustomFloat7:  7,
					CustomFloat8:  8,
					CustomFloat9:  9,
					CustomFloat10: 10,
					CustomFloat11: 11,
					CustomFloat12: 12,
					CustomFloat13: 13,
					CustomFloat14: 14,
					CustomFloat15: 15,
					CustomFloat16: 16,
					CustomFloat17: 17,
					CustomFloat18: 18,
					CustomFloat19: 19,
					CustomFloat20: 20,
					CustomFloat21: 21,
					CustomFloat22: 22,
					CustomFloat23: 23,

					EmployeeType:   "1",
					EmploymentType: "Contract",
					Fee:            123,
					JobOrder: NestedEntity{
						ID:    99,
						Title: "Job Title ABC",
					},
					OnboardingStatus: "Completed",
					ReferralFee:      25,
					ReferralFeeType:  "percentage",
					Status:           "Active",
				},
				{
					ID: 2,

					DateAdded:        EpochMilli(1659190221000),
					DateBegin:        EpochMilli(1659290221000),
					DateEnd:          EpochMilli(1659990221000),
					DateLastModified: EpochMilli(1659193221000),

					CustomDate1:  EpochMilli(0),
					CustomDate2:  EpochMilli(1659294221000),
					CustomDate3:  EpochMilli(1659295221000),
					CustomDate4:  EpochMilli(1659296221000),
					CustomDate5:  EpochMilli(1659297221000),
					CustomDate6:  EpochMilli(1659298221000),
					CustomDate7:  EpochMilli(1659299221000),
					CustomDate8:  EpochMilli(1659300221000),
					CustomDate9:  EpochMilli(1659301221000),
					CustomDate10: EpochMilli(1659302221000),
					CustomDate11: EpochMilli(1659303221000),
					CustomDate12: EpochMilli(1659304221000),
					CustomDate13: EpochMilli(1659305221000),

					CustomText1:   "",
					CustomText2:   "text82",
					CustomText3:   "text83",
					CustomText4:   "text84",
					CustomText5:   "text85",
					CustomText6:   "text86",
					CustomText7:   "text87",
					CustomText8:   "text88",
					CustomText9:   "text89",
					CustomText10:  "text810",
					CustomText11:  "text811",
					CustomText12:  "text812",
					CustomText13:  "text813",
					CustomText14:  "text814",
					CustomText15:  "text815",
					CustomText16:  "text816",
					CustomText17:  "text817",
					CustomText18:  "text818",
					CustomText19:  "text819",
					CustomText20:  "text820",
					CustomText21:  "text821",
					CustomText22:  "text822",
					CustomText23:  "text823",
					CustomText24:  "text824",
					CustomText25:  "text825",
					CustomText26:  "text826",
					CustomText27:  "text827",
					CustomText28:  "text828",
					CustomText29:  "text829",
					CustomText30:  "text830",
					CustomText31:  "text831",
					CustomText32:  "text832",
					CustomText33:  "text833",
					CustomText34:  "text834",
					CustomText35:  "text835",
					CustomText36:  "text836",
					CustomText37:  "text837",
					CustomText38:  "text838",
					CustomText39:  "text839",
					CustomText40:  "text840",
					CustomText41:  "text841",
					CustomText42:  "text842",
					CustomText43:  "text843",
					CustomText44:  "text844",
					CustomText45:  "text845",
					CustomText46:  "text846",
					CustomText47:  "text847",
					CustomText48:  "text848",
					CustomText49:  "text849",
					CustomText50:  "text850",
					CustomText51:  "text851",
					CustomText52:  "text852",
					CustomText53:  "text853",
					CustomText54:  "text854",
					CustomText55:  "text855",
					CustomText56:  "text856",
					CustomText57:  "text857",
					CustomText58:  "text858",
					CustomText59:  "text859",
					CustomText60:  "text860",
					CustomFloat1:  80,
					CustomFloat2:  82,
					CustomFloat3:  83,
					CustomFloat4:  84,
					CustomFloat5:  85,
					CustomFloat6:  86,
					CustomFloat7:  87,
					CustomFloat8:  88,
					CustomFloat9:  89,
					CustomFloat10: 810,
					CustomFloat11: 811,
					CustomFloat12: 812,
					CustomFloat13: 813,
					CustomFloat14: 814,
					CustomFloat15: 815,
					CustomFloat16: 816,
					CustomFloat17: 817,
					CustomFloat18: 818,
					CustomFloat19: 819,
					CustomFloat20: 820,
					CustomFloat21: 821,
					CustomFloat22: 822,
					CustomFloat23: 823,

					EmployeeType:   "1",
					EmploymentType: "Part-time",
					Fee:            50,
					JobOrder: NestedEntity{
						ID:    102,
						Title: "Job Title CDE",
					},
					OnboardingStatus: "Started",
					ReferralFee:      0,
					ReferralFeeType:  "percentage",
					Status:           "Completed",
				},
			},
		}

		server := buildMockServer(func(w http.ResponseWriter, r *http.Request) {
			assert.Equal(t, r.Header.Get("BhRestToken"), "tok-456")
			assert.Equal(t, r.URL.Query().Get("fields"), "id,dateAdded,customText1")
			assert.Equal(t, r.URL.Query().Get("where"), "id>0")
			assert.Equal(t, r.URL.Query().Get("start"), "0")
			assert.Equal(t, r.URL.Query().Get("count"), "200")

			json.NewEncoder(w).Encode(want)
		})

		defer server.Close()

		job := &placementService{client: &Client{client: &http.Client{}, token: "tok-456"}, baseURL: server.URL}
		query := SearchQuery{
			Fields: []string{"id", "dateAdded", "customText1"},
			Where:  "id>0",
			Start:  0,
			Count:  200,
		}

		got, err := job.Search(context.Background(), query)
		assert.NilError(t, err)
		assert.DeepEqual(t, got, &want)
	})

	t.Run("returns error when request fails", func(t *testing.T) {
		jos := &placementService{client: New("")}

		_, err := jos.Search(context.Background(), SearchQuery{})
		assert.ErrorContains(t, err, "unsupported protocol scheme")
	})

	t.Run("returns error when request building fail", func(t *testing.T) {
		jos := &placementService{client: &Client{}, baseURL: string([]byte{0x7f})}
		_, err := jos.Search(context.Background(), SearchQuery{})
		assert.ErrorContains(t, err, "net/url: invalid control character in URL")
	})

	t.Run("returns error when non 200 response code", func(t *testing.T) {
		server := buildMockServer(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			io.WriteString(w, "error invalid query")
		})
		defer server.Close()

		jos := &placementService{client: New(""), baseURL: server.URL}

		_, err := jos.Search(context.Background(), SearchQuery{})
		want := &Error{
			StatusCode:  http.StatusBadRequest,
			RequestPath: "/query/Placement",
			Message:     "error invalid query",
		}
		assert.DeepEqual(t, err, want)
	})
}
