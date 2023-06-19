package entity

type Asset struct {
	ID           string
	Name         string
	MarketVolume int
}

func NewAsset(id string, name string, MarketVolume int) *Asset {
	return &Asset{
		ID:           id,
		Name:         name,
		MarketVolume: MarketVolume,
	}
}
