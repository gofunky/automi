package api

type DefaultProcess struct {}



func (p *DefaultProcess) Exec(f ProcFunc) error {

	if f != nil {
		return f( )
	}

}