package bullhorn

import (
	"context"
	"net/url"
	"strconv"
	"strings"
)

type PlacementService interface {
	Search(context.Context, SearchQuery) (*Placements, error)
}

type placementService struct {
	baseURL string
	client  *Client
}

type Placements struct {
	Items []Placement `json:"data"`
}

type Placement struct {
	ID int `json:"id"`

	DateAdded        EpochMilli `json:"dateAdded"`
	DateBegin        EpochMilli `json:"dateBegin"`
	DateEnd          EpochMilli `json:"dateEnd,omitempty"`
	DateLastModified EpochMilli `json:"dateLastModified"`

	// Custom date fields
	CustomDate1  EpochMilli `json:"customDate1"`
	CustomDate2  EpochMilli `json:"customDate2"`
	CustomDate3  EpochMilli `json:"customDate3"`
	CustomDate4  EpochMilli `json:"customDate4"`
	CustomDate5  EpochMilli `json:"customDate5"`
	CustomDate6  EpochMilli `json:"customDate6"`
	CustomDate7  EpochMilli `json:"customDate7"`
	CustomDate8  EpochMilli `json:"customDate8"`
	CustomDate9  EpochMilli `json:"customDate9"`
	CustomDate10 EpochMilli `json:"customDate10"`
	CustomDate11 EpochMilli `json:"customDate11"`
	CustomDate12 EpochMilli `json:"customDate12"`
	CustomDate13 EpochMilli `json:"customDate13"`

	// Custom text fields
	CustomText1  string `json:"customText1"`
	CustomText2  string `json:"customText2"`
	CustomText3  string `json:"customText3"`
	CustomText4  string `json:"customText4"`
	CustomText5  string `json:"customText5"`
	CustomText6  string `json:"customText6"`
	CustomText7  string `json:"customText7"`
	CustomText8  string `json:"customText8"`
	CustomText9  string `json:"customText9"`
	CustomText10 string `json:"customText10"`
	CustomText11 string `json:"customText11"`
	CustomText12 string `json:"customText12"`
	CustomText13 string `json:"customText13"`
	CustomText14 string `json:"customText14"`
	CustomText15 string `json:"customText15"`
	CustomText16 string `json:"customText16"`
	CustomText17 string `json:"customText17"`
	CustomText18 string `json:"customText18"`
	CustomText19 string `json:"customText19"`
	CustomText20 string `json:"customText20"`
	CustomText21 string `json:"customText21"`
	CustomText22 string `json:"customText22"`
	CustomText23 string `json:"customText23"`
	CustomText24 string `json:"customText24"`
	CustomText25 string `json:"customText25"`
	CustomText26 string `json:"customText26"`
	CustomText27 string `json:"customText27"`
	CustomText28 string `json:"customText28"`
	CustomText29 string `json:"customText29"`
	CustomText30 string `json:"customText30"`
	CustomText31 string `json:"customText31"`
	CustomText32 string `json:"customText32"`
	CustomText33 string `json:"customText33"`
	CustomText34 string `json:"customText34"`
	CustomText35 string `json:"customText35"`
	CustomText36 string `json:"customText36"`
	CustomText37 string `json:"customText37"`
	CustomText38 string `json:"customText38"`
	CustomText39 string `json:"customText39"`
	CustomText40 string `json:"customText40"`
	CustomText41 string `json:"customText41"`
	CustomText42 string `json:"customText42"`
	CustomText43 string `json:"customText43"`
	CustomText44 string `json:"customText44"`
	CustomText45 string `json:"customText45"`
	CustomText46 string `json:"customText46"`
	CustomText47 string `json:"customText47"`
	CustomText48 string `json:"customText48"`
	CustomText49 string `json:"customText49"`
	CustomText50 string `json:"customText50"`
	CustomText51 string `json:"customText51"`
	CustomText52 string `json:"customText52"`
	CustomText53 string `json:"customText53"`
	CustomText54 string `json:"customText54"`
	CustomText55 string `json:"customText55"`
	CustomText56 string `json:"customText56"`
	CustomText57 string `json:"customText57"`
	CustomText58 string `json:"customText58"`
	CustomText59 string `json:"customText59"`
	CustomText60 string `json:"customText60"`

	// Custom float fields
	CustomFloat1  float64 `json:"customFloat1"`
	CustomFloat2  float64 `json:"customFloat2"`
	CustomFloat3  float64 `json:"customFloat3"`
	CustomFloat4  float64 `json:"customFloat4"`
	CustomFloat5  float64 `json:"customFloat5"`
	CustomFloat6  float64 `json:"customFloat6"`
	CustomFloat7  float64 `json:"customFloat7"`
	CustomFloat8  float64 `json:"customFloat8"`
	CustomFloat9  float64 `json:"customFloat9"`
	CustomFloat10 float64 `json:"customFloat10"`
	CustomFloat11 float64 `json:"customFloat11"`
	CustomFloat12 float64 `json:"customFloat12"`
	CustomFloat13 float64 `json:"customFloat13"`
	CustomFloat14 float64 `json:"customFloat14"`
	CustomFloat15 float64 `json:"customFloat15"`
	CustomFloat16 float64 `json:"customFloat16"`
	CustomFloat17 float64 `json:"customFloat17"`
	CustomFloat18 float64 `json:"customFloat18"`
	CustomFloat19 float64 `json:"customFloat19"`
	CustomFloat20 float64 `json:"customFloat20"`
	CustomFloat21 float64 `json:"customFloat21"`
	CustomFloat22 float64 `json:"customFloat22"`
	CustomFloat23 float64 `json:"customFloat23"`

	EmployeeType     string       `json:"employeeType"`
	EmploymentType   string       `json:"employmentType"`
	Fee              float64      `json:"fee"`
	JobOrder         NestedEntity `json:"jobOrder"`
	OnboardingStatus string       `json:"onboardingStatus"`
	ReferralFee      float64      `json:"referralFee"`
	ReferralFeeType  string       `json:"referralFeeType"`
	Status           string       `json:"status"`
}

func (j *placementService) Search(ctx context.Context, query SearchQuery) (*Placements, error) {
	q := url.Values{}
	q.Add("fields", strings.Join(query.Fields, ","))
	q.Add("where", query.Where)
	q.Add("start", strconv.Itoa(query.Start))
	q.Add("count", strconv.Itoa(query.Count))

	req, err := j.client.buildGETRequest(j.client.buildURL(j.baseURL, "/query/Placement", q))
	if err != nil {
		return nil, err
	}

	placements := &Placements{}
	if err := j.client.doRequest(req.WithContext(ctx), placements); err != nil {
		return nil, err
	}

	return placements, nil
}
