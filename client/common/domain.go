package common

import (
	"encoding/csv"
	"os"
)

// Bet representa una apuesta simple del ejercicio 5.
type Bet struct {
	AgencyID  string
	FirstName string
	LastName  string
	DNI       string
	Birthdate string
	Number    string
}

// NewBetFromEnv crea una apuesta tomando los datos de las variables de entorno y el id de agencia.
func NewBetFromEnv(agencyID string) Bet {
	return Bet{
		AgencyID:  agencyID,
		FirstName: os.Getenv("NOMBRE"),
		LastName:  os.Getenv("APELLIDO"),
		DNI:       os.Getenv("DOCUMENTO"),
		Birthdate: os.Getenv("NACIMIENTO"),
		Number:    os.Getenv("NUMERO"),
	}
}

// LoadBetsFromCSV carga todas las apuestas de un archivo CSV de agencia.
// Formato esperado por línea: nombre,apellido,dni,nacimiento,numero
func LoadBetsFromCSV(path string, agencyID string) ([]Bet, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	bets := make([]Bet, 0, len(records))
	for _, row := range records {
		if len(row) < 5 {
			continue
		}
		bets = append(bets, Bet{
			AgencyID:  agencyID,
			FirstName: row[0],
			LastName:  row[1],
			DNI:       row[2],
			Birthdate: row[3],
			Number:    row[4],
		})
	}
	return bets, nil
}
