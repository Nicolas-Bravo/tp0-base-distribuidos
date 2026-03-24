package common

import "os"

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
