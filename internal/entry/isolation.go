package entry

type Isolation struct {
	OriginAddr      string
	OriginUser      string "Default"
	TargetAddr      string
	TargetUser      string "Default"
	Prefix          string
	OriginIsCluster bool
	TargetIsCLuster bool
	BySync          bool
}
