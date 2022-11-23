package db

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"rest/internal/user"
	"rest/pkg/logging"
)

type db struct {
	collection *mongo.Collection
	logger     *logging.Logger
}

func (d db) Create(ctx context.Context, user user.User) (string, error) {
	d.logger.Debug("create user")
	result, err := d.collection.InsertOne(ctx, user)
	if err != nil {
		return "", fmt.Errorf("failed to create: %v", err)
	}

	d.logger.Debug("convert InsertedID to ObjectID")
	oid, ok := result.InsertedID.(primitive.ObjectID)
	if ok {
		return oid.Hex(), nil
	}
	d.logger.Trace(user)
	return "", fmt.Errorf("failed to convert oid to hex")
}

func (d db) FindOne(ctx context.Context, id string) (user.User, error) {
	u := user.User{}
	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return u, fmt.Errorf("failed to convert hex %s to objectid", id)
	}

	filter := bson.M{"_id": oid}
	result := d.collection.FindOne(ctx, filter)
	if result.Err() != nil {
		if errors.Is(result.Err(), mongo.ErrNoDocuments) {
			return u, fmt.Errorf("not found")
		}
		return u, fmt.Errorf("failed find user by id: %s due to err: %v", id, err)
	}
	if err = result.Decode(&u); err != nil {
		return u, fmt.Errorf("failed to decode user by id: %s from DB due to err: %v", id, err)
	}
	return u, nil
}

func (d db) Update(ctx context.Context, user user.User) error {
	objectID, err := primitive.ObjectIDFromHex(user.ID)
	if err != nil {
		return fmt.Errorf("failed to convert user ID to object ID (id=%s)", user.ID)
	}
	filter := bson.M{"_id": objectID}

	userBytes, err := bson.Marshal(user)
	if err != nil {
		return fmt.Errorf("failed to serialize user: %v", err)
	}

	var updateUserObj bson.M
	err = bson.Unmarshal(userBytes, &updateUserObj)
	if err != nil {
		return fmt.Errorf("failed to deserialize user: %v", err)
	}

	delete(updateUserObj, "_id")

	update := bson.M{
		"$set": updateUserObj,
	}

	result, err := d.collection.UpdateOne(ctx, filter, update)
	if err != nil {
		return fmt.Errorf("failed to update user query: %v", err)
	}
	if result.MatchedCount == 0 {
		return fmt.Errorf("not found")
	}
	d.logger.Tracef("matched: %d, modified: %d", result.MatchedCount, result.MatchedCount)
	return nil
}

func (d db) Delete(ctx context.Context, id string) error {
	objectIDFromHex, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return fmt.Errorf("failed to convert user ID to hex: %d", id)
	}

	filter := bson.M{"_id": objectIDFromHex}

	result, err := d.collection.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("faield to execute delete: %v", err)
	}
	if result.DeletedCount == 0 {
		return fmt.Errorf("not found")
	}
	d.logger.Tracef("deleted %d docs", result.DeletedCount)
	return nil
}

func NewStorage(database *mongo.Database, collection string, logger *logging.Logger) user.Storage {
	return &db{
		collection: database.Collection(collection),
		logger:     logger,
	}
}
