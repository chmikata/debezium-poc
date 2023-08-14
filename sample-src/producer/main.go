package main

import (
	"context"
	"database/sql"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/chmikata/debezium-poc/sample-src/producer/models"
	_ "github.com/lib/pq"
	"github.com/volatiletech/sqlboiler/v4/boil"

	echo "github.com/labstack/echo/v4"
	midleware "github.com/labstack/echo/v4/middleware"
	"github.com/labstack/gommon/log"
)

type CustomContext struct {
	echo.Context
	db *sql.DB
}

func customContextMiddleware(db *sql.DB) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			cc := &CustomContext{
				Context: c,
				db:      db,
			}
			return next(cc)
		}
	}
}

func initDB() *sql.DB {
	db, err := sql.Open("postgres", "host=localhost port=5432 user=postgres password=password dbname=db1 sslmode=disable")
	if err != nil {
		panic(err)
	}
	return db
}

func main() {
	e := echo.New()

	db := initDB()
	defer db.Close()

	e.Use(midleware.Logger())
	e.Use(midleware.Recover())
	e.Use(customContextMiddleware(db))

	if l, ok := e.Logger.(*log.Logger); ok {
		l.SetLevel(log.INFO)
	}

	e.GET("/produce", produceMessage)

	errC := make(chan error)
	go func() {
		if err := e.Start(":8080"); err != nil {
			errC <- err
		}
	}()

	quitC := make(chan os.Signal)
	signal.Notify(quitC, syscall.SIGTERM, syscall.SIGINT)
	select {
	case err := <-errC:
		e.Logger.Fatal(err)
		panic(err)
	case <-quitC:
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		e.Logger.Info("Shutting down server...")
		if err := e.Shutdown(shutdownCtx); err != nil {
			errC <- err
		}
	}
}

func produceMessage(e echo.Context) error {
	cc, ok := e.(*CustomContext)
	if !ok {
		return echo.NewHTTPError(500, "failed to cast context")
	}

	var wg sync.WaitGroup

	for i := 0; i < 1; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			regist := &models.Regist{
				Name: "test:" + strconv.Itoa(i),
			}
			err := regist.Insert(context.Background(), cc.db, boil.Infer())
			if err != nil {
				cc.Logger().Errorf("failed to insert regist: %v", err)
			}
			account := &models.Account{
				ID:   regist.ID,
				Name: "test:" + strconv.Itoa(i),
			}
			err = account.Insert(context.Background(), cc.db, boil.Infer())
			if err != nil {
				cc.Logger().Errorf("failed to insert account: %v", err)
			}
			cc.Logger().Infof("regist ID: %v", account.ID)
		}()
	}

	wg.Wait()

	return nil
}
